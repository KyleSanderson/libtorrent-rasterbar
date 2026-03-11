/*

Copyright (c) 2024, libtorrent contributors
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the distribution.
    * Neither the name of the author nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

#include "libtorrent/config.hpp"

#if TORRENT_HAVE_IO_URING

#include "libtorrent/settings_pack.hpp"
#include "libtorrent/aux_/io_uring_storage.hpp"
#include "libtorrent/aux_/path.hpp"
#include "libtorrent/aux_/open_mode.hpp"
#include "libtorrent/torrent_status.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace libtorrent::flags; // for flag operators

// make sure 64-bit file operations are available
static_assert(sizeof(off_t) >= 8, "64 bit file operations are required");

namespace libtorrent {
namespace aux {

	// --- io_uring_fd_pool implementation ---

	int io_uring_fd_pool::open_file(std::string const& path, int flags
		, mode_t mode, storage_error& ec)
	{
		std::lock_guard<std::mutex> l(m_mutex);

		// check if already open
		for (auto& e : m_entries)
		{
			if (e.path == path && e.flags == flags)
			{
				e.last_use = ++m_counter;
				return e.fd;
			}
		}

		// evict if at capacity
		while (static_cast<int>(m_entries.size()) >= m_max_size)
			evict_lru();

		int fd = ::open(path.c_str(), flags, mode);
		if (fd < 0)
		{
			ec.ec.assign(errno, generic_category());
			ec.operation = operation_t::file_open;
			return -1;
		}

		fd_entry entry;
		entry.fd = fd;
		entry.path = path;
		entry.flags = flags;
		entry.last_use = ++m_counter;
		m_entries.push_back(std::move(entry));
		return fd;
	}

	void io_uring_fd_pool::evict_lru()
	{
		if (m_entries.empty()) return;
		auto oldest = m_entries.begin();
		for (auto it = m_entries.begin(); it != m_entries.end(); ++it)
		{
			if (it->last_use < oldest->last_use)
				oldest = it;
		}
		if (oldest->fd >= 0)
			::close(oldest->fd);
		m_entries.erase(oldest);
	}

	void io_uring_fd_pool::close_all()
	{
		std::lock_guard<std::mutex> l(m_mutex);
		for (auto& e : m_entries)
		{
			if (e.fd >= 0) ::close(e.fd);
		}
		m_entries.clear();
	}

	void io_uring_fd_pool::close_for_storage(storage_index_t st)
	{
		std::lock_guard<std::mutex> l(m_mutex);
		auto it = m_entries.begin();
		while (it != m_entries.end())
		{
			if (it->storage == st)
			{
				if (it->fd >= 0) ::close(it->fd);
				it = m_entries.erase(it);
			}
			else
			{
				++it;
			}
		}
	}

	// --- io_uring_storage implementation ---

	io_uring_storage::io_uring_storage(storage_params const& p)
		: m_files(p.files)
		, m_save_path(p.path)
		, m_file_priority(p.priorities)
		, m_part_file_name("." + to_hex(p.info_hash) + ".parts")
	{
		if (p.mapped_files) m_mapped_files.reset(new file_storage(*p.mapped_files));
	}

	file_storage const& io_uring_storage::files() const
	{
		return m_mapped_files ? *m_mapped_files.get() : m_files;
	}

	io_uring_storage::~io_uring_storage()
	{
		error_code ec;
		if (m_part_file) m_part_file->flush_metadata(ec);
		release_files();
	}

	void io_uring_storage::need_partfile()
	{
		if (m_part_file) return;
		m_part_file = std::make_unique<posix_part_file>(
			m_save_path, m_part_file_name
			, files().num_pieces(), files().piece_length());
	}

	int io_uring_storage::open_file_impl(file_index_t idx, int flags, storage_error& ec)
	{
		std::string const fn = files().file_path(idx, m_save_path);

		int fd = ::open(fn.c_str(), flags, 0644);
		if (fd < 0)
		{
			ec.ec.assign(errno, generic_category());

			if ((flags & O_WRONLY || flags & O_RDWR)
				&& (ec.ec == boost::system::errc::no_such_file_or_directory))
			{
				ec.ec.clear();
				create_directories(parent_path(fn), ec.ec);
				if (ec.ec)
				{
					ec.file(idx);
					ec.operation = operation_t::mkdir;
					return -1;
				}

				fd = ::open(fn.c_str(), flags | O_CREAT, 0644);
				if (fd < 0)
				{
					ec.ec.assign(errno, generic_category());
					ec.file(idx);
					ec.operation = operation_t::file_open;
					return -1;
				}
			}
			else
			{
				ec.file(idx);
				ec.operation = operation_t::file_open;
				return -1;
			}
		}

		return fd;
	}

	int io_uring_storage::open_file_fd(file_index_t idx, bool write_mode, storage_error& ec)
	{
		int const file_idx = static_cast<int>(idx);

		auto it = m_open_files.find(file_idx);
		if (it != m_open_files.end())
			return it->second;

		int flags = write_mode ? (O_RDWR | O_CREAT) : O_RDONLY;
		int fd = open_file_impl(idx, flags, ec);
		if (fd >= 0)
			m_open_files[file_idx] = fd;
		return fd;
	}

	std::vector<io_uring_storage::file_mapping_result>
	io_uring_storage::map_piece_to_files(
		piece_index_t piece, int offset, int length, bool write_mode
		, storage_error& ec)
	{
		std::vector<file_mapping_result> result;
		file_storage const& fs = files();

		// map from piece-space to file-space
		// a single piece may span multiple files
		std::int64_t const piece_offset = std::int64_t(static_cast<int>(piece))
			* fs.piece_length() + offset;

		auto const file_slices = fs.map_block(piece, offset, length);
		for (auto const& s : file_slices)
		{
			file_mapping_result r;
			r.is_pad_file = fs.pad_file_at(s.file_index);
			r.file_offset = s.offset;
			r.length = static_cast<int>(s.size);

			if (r.is_pad_file)
			{
				r.fd = -1;
			}
			else if (s.file_index < m_file_priority.end_index()
				&& m_file_priority[s.file_index] == dont_download
				&& use_partfile(s.file_index))
			{
				// Data for this file is routed through the partfile. Do NOT open
				// the real file fd for io_uring; let the sync fallback handle it.
				r.fd = -1;
				r.uses_partfile = true;
			}
			else
			{
				r.fd = open_file_fd(s.file_index, write_mode, ec);
				if (ec.ec) return {};
			}
			result.push_back(r);
		}

		return result;
	}

	int io_uring_storage::read(settings_interface const& sett
		, span<char> buffer
		, piece_index_t const piece, int const offset
		, storage_error& error)
	{
		return readwrite(files(), buffer, piece, offset, error
			, [this](file_index_t const file_index
				, std::int64_t const file_offset
				, span<char> buf, storage_error& ec) -> int
		{
			if (files().pad_file_at(file_index))
				return aux::read_zeroes(buf);

			if (file_index < m_file_priority.end_index()
				&& m_file_priority[file_index] == dont_download
				&& use_partfile(file_index))
			{
				TORRENT_ASSERT(m_part_file);
				error_code e;
				peer_request map = files().map_file(file_index, file_offset, 0);
				int const ret = m_part_file->read(buf, map.piece, map.start, e);
				if (e)
				{
					ec.ec = e;
					ec.operation = operation_t::partfile_read;
					return -1;
				}
				return ret;
			}

			int fd = open_file_fd(file_index, false, ec);
			if (ec.ec) return -1;

			ec.operation = operation_t::file_read;

			ssize_t const r = ::pread(fd, buf.data()
				, static_cast<std::size_t>(buf.size())
				, static_cast<off_t>(file_offset));
			if (r < 0)
			{
				ec.ec.assign(errno, generic_category());
				return -1;
			}
			if (r == 0)
			{
				ec.ec.assign(errors::file_too_short, libtorrent_category());
				return -1;
			}
			return static_cast<int>(r);
		});
	}

	int io_uring_storage::write(settings_interface const& sett
		, span<char> buffer
		, piece_index_t const piece, int const offset
		, storage_error& error)
	{
		return readwrite(files(), buffer, piece, offset, error
			, [this](file_index_t const file_index
				, std::int64_t const file_offset
				, span<char> buf, storage_error& ec) -> int
		{
			if (files().pad_file_at(file_index))
				return int(buf.size());

			if (file_index < m_file_priority.end_index()
				&& m_file_priority[file_index] == dont_download
				&& use_partfile(file_index))
			{
				TORRENT_ASSERT(m_part_file);
				error_code e;
				peer_request map = files().map_file(file_index, file_offset, 0);
				int const ret = m_part_file->write(buf, map.piece, map.start, e);
				if (e)
				{
					ec.ec = e;
					ec.operation = operation_t::partfile_write;
				}
				return ret;
			}

			int fd = open_file_fd(file_index, true, ec);
			if (ec.ec) return -1;

			ec.operation = operation_t::file_write;

			ssize_t const r = ::pwrite(fd, buf.data()
				, static_cast<std::size_t>(buf.size())
				, static_cast<off_t>(file_offset));
			if (r < 0)
			{
				ec.ec.assign(errno, generic_category());
				return -1;
			}
			if (r != static_cast<ssize_t>(buf.size()))
			{
				ec.ec.assign(errors::file_too_short, libtorrent_category());
			}

			m_stat_cache.set_dirty(file_index);
			return static_cast<int>(r);
		});
	}

	void io_uring_storage::set_file_priority(settings_interface const&
		, aux::vector<download_priority_t, file_index_t>& prio
		, storage_error& ec)
	{
		if (prio.size() > m_file_priority.size())
			m_file_priority.resize(prio.size(), default_priority);

		file_storage const& fs = files();
		for (file_index_t i(0); i < prio.end_index(); ++i)
		{
			if (fs.pad_file_at(i)) continue;

			download_priority_t const old_prio = m_file_priority[i];
			download_priority_t new_prio = prio[i];
			if (old_prio == dont_download && new_prio != dont_download)
			{
				if (m_part_file && use_partfile(i))
				{
					m_part_file->export_file([this, i, &ec](std::int64_t file_offset, span<char> buf)
					{
						storage_error se;
						int fd = open_file_fd(i, true, se);
						if (se.ec) { ec = se; return; }
						ssize_t const r = ::pwrite(fd, buf.data()
							, static_cast<std::size_t>(buf.size())
							, static_cast<off_t>(file_offset));
						if (r < 0)
						{
							ec.ec.assign(errno, generic_category());
							return;
						}
						if (r != static_cast<ssize_t>(buf.size()))
						{
							ec.ec.assign(errors::file_too_short, libtorrent_category());
						}
					}, fs.file_offset(i), fs.file_size(i), ec.ec);

					if (ec)
					{
						ec.file(i);
						ec.operation = operation_t::partfile_write;
						prio = m_file_priority;
						return;
					}
				}
			}
			else if (old_prio != dont_download && new_prio == dont_download)
			{
				std::string const fp = fs.file_path(i, m_save_path);
				bool const file_exists = exists(fp, ec.ec);
				if (ec.ec)
				{
					ec.file(i);
					ec.operation = operation_t::file_stat;
					prio = m_file_priority;
					return;
				}
				use_partfile(i, !file_exists);
			}
			ec.ec.clear();
			m_file_priority[i] = new_prio;

			if (m_file_priority[i] == dont_download && use_partfile(i))
			{
				need_partfile();
			}
		}
		if (m_part_file) m_part_file->flush_metadata(ec.ec);
		if (ec)
		{
			ec.file(torrent_status::error_file_partfile);
			ec.operation = operation_t::partfile_write;
		}
	}

	bool io_uring_storage::has_any_file(storage_error& error)
	{
		m_stat_cache.reserve(files().num_files());
		return aux::has_any_file(files(), m_save_path, m_stat_cache, error);
	}

	bool io_uring_storage::verify_resume_data(add_torrent_params const& rd
		, vector<std::string, file_index_t> const& links
		, storage_error& ec)
	{
		return aux::verify_resume_data(rd, links, files()
			, m_file_priority, m_stat_cache, m_save_path, ec);
	}

	void io_uring_storage::release_files()
	{
		// close all cached file descriptors
		for (auto& p : m_open_files)
		{
			if (p.second >= 0) ::close(p.second);
		}
		m_open_files.clear();

		m_stat_cache.clear();
		if (m_part_file)
		{
			error_code ignore;
			m_part_file->flush_metadata(ignore);
		}
	}

	void io_uring_storage::delete_files(remove_flags_t const options, storage_error& error)
	{
		release_files();
		if (m_part_file) m_part_file.reset();
		aux::delete_files(files(), m_save_path, m_part_file_name, options, error);
	}

	std::pair<status_t, std::string> io_uring_storage::move_storage(std::string const& sp
		, move_flags_t const flags, storage_error& ec)
	{
		release_files();
		lt::status_t ret;
		auto move_partfile = [&](std::string const& new_save_path, error_code& e)
		{
			if (!m_part_file) return;
			m_part_file->move_partfile(new_save_path, e);
		};
		std::tie(ret, m_save_path) = aux::move_storage(files(), m_save_path, sp
			, std::move(move_partfile), flags, ec);

		m_stat_cache.clear();
		return { ret, m_save_path };
	}

	void io_uring_storage::rename_file(file_index_t const index
		, std::string const& new_filename, storage_error& ec)
	{
		if (index < file_index_t(0) || index >= files().end_file()) return;
		std::string const old_name = files().file_path(index, m_save_path);

		if (exists(old_name, ec.ec))
		{
			std::string new_path;
			if (is_complete(new_filename)) new_path = new_filename;
			else new_path = combine_path(m_save_path, new_filename);
			std::string new_dir = parent_path(new_path);

			error_code best_effort;
			if (exists(new_path, best_effort))
			{
				ec.ec = error_code(boost::system::errc::file_exists, generic_category());
				ec.file(index);
				ec.operation = operation_t::file_rename;
				return;
			}

			create_directories(new_dir, ec.ec);
			if (ec.ec)
			{
				ec.file(index);
				ec.operation = operation_t::file_rename;
				return;
			}

			rename(old_name, new_path, ec.ec);

			if (ec.ec == boost::system::errc::no_such_file_or_directory)
				ec.ec.clear();

			if (ec)
			{
				ec.file(index);
				ec.operation = operation_t::file_rename;
				return;
			}
		}
		else if (ec.ec)
		{
			ec.file(index);
			ec.operation = operation_t::file_rename;
			return;
		}

		if (!m_mapped_files)
		{
			m_mapped_files.reset(new file_storage(files()));
		}
		m_mapped_files->rename_file(index, new_filename);
	}

	status_t io_uring_storage::initialize(settings_interface const&, storage_error& ec)
	{
		m_stat_cache.reserve(files().num_files());

		file_storage const& fs = files();
		status_t ret{};
		for (file_index_t i(0); i < m_file_priority.end_index(); ++i)
		{
			if (m_file_priority[i] != dont_download || fs.pad_file_at(i))
				continue;

			file_status s;
			std::string const file_path = fs.file_path(i, m_save_path);
			error_code err;
			stat_file(file_path, &s, err);

			if (s.file_size > fs.file_size(i))
				ret = ret | status_t::oversized_file;

			if (!err)
			{
				use_partfile(i, false);
			}
			else
			{
				need_partfile();
			}
		}

		aux::initialize_storage(fs, m_save_path, m_stat_cache, m_file_priority
			, [this](file_index_t const file_index, storage_error& e)
			{
				int fd = open_file_impl(file_index, O_RDWR | O_CREAT, e);
				if (fd >= 0) ::close(fd);
			}
			, aux::create_symlink
			, [&ret](file_index_t, std::int64_t) { ret = ret | status_t::oversized_file; }
			, ec);
		return ret;
	}

	bool io_uring_storage::use_partfile(file_index_t const index) const
	{
		TORRENT_ASSERT_VAL(index >= file_index_t{}, index);
		if (index >= m_use_partfile.end_index()) return true;
		return m_use_partfile[index];
	}

	void io_uring_storage::use_partfile(file_index_t const index, bool const b)
	{
		if (index >= m_use_partfile.end_index())
		{
			if (b) return;
			m_use_partfile.resize(static_cast<int>(index) + 1, true);
		}
		m_use_partfile[index] = b;
	}

} // namespace aux
} // namespace libtorrent

#endif // TORRENT_HAVE_IO_URING
