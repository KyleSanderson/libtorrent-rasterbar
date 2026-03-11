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

#ifndef TORRENT_IO_URING_STORAGE_HPP
#define TORRENT_IO_URING_STORAGE_HPP

#include "libtorrent/config.hpp"

#if TORRENT_HAVE_IO_URING

#include "libtorrent/stat_cache.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/storage_defs.hpp"
#include "libtorrent/aux_/storage_utils.hpp"
#include "libtorrent/hex.hpp"
#include "libtorrent/aux_/open_mode.hpp"
#include "libtorrent/aux_/posix_part_file.hpp"
#include "libtorrent/file.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>

namespace libtorrent {
namespace aux {

	// fd_pool: a simple LRU cache of open file descriptors
	struct TORRENT_EXTRA_EXPORT io_uring_fd_pool
	{
		io_uring_fd_pool() : m_max_size(40) {}
		explicit io_uring_fd_pool(int max_size) : m_max_size(max_size) {}

		// returns a file descriptor for the given file, opening it if needed
		// the returned fd is owned by the pool and must not be closed by the caller
		int open_file(std::string const& path, int flags, mode_t mode
			, storage_error& ec);

		void close_all();
		void close_for_storage(storage_index_t st);
		void set_max_size(int s) { m_max_size = s; }

		struct fd_entry
		{
			int fd = -1;
			std::string path;
			int flags = 0;
			storage_index_t storage{};
			std::uint64_t last_use = 0;
		};

	private:
		void evict_lru();
		std::vector<fd_entry> m_entries;
		int m_max_size;
		std::uint64_t m_counter = 0;
		std::mutex m_mutex;
	};

	struct TORRENT_EXTRA_EXPORT io_uring_storage
	{
		explicit io_uring_storage(storage_params const& p);
		file_storage const& files() const;
		~io_uring_storage();

		// synchronous read/write using pread/pwrite (for use by io_uring thread)
		int read(settings_interface const& sett
			, span<char> buf
			, piece_index_t piece, int offset
			, storage_error& error);

		int write(settings_interface const& sett
			, span<char> buf
			, piece_index_t piece, int offset
			, storage_error& error);

		// returns the file descriptor and absolute file offset for a given
		// piece offset. Used by the io_uring disk I/O to submit SQEs directly.
		struct file_mapping_result
		{
			int fd;
			std::int64_t file_offset;
			int length; // clamped to file boundary
			bool is_pad_file;
			// true when this file's data is routed through a partfile
			// (file priority == 0 and use_partfile is set). The io_uring
			// fast path must skip these; the sync st->read()/st->write()
			// path handles them correctly.
			bool uses_partfile{false};
		};

		// open a file and return its fd (for io_uring SQE submission)
		int open_file_fd(file_index_t idx, bool write_mode, storage_error& ec);

		// map a piece+offset to file_index + file_offset
		// (thin wrapper around file_storage::map_file)
		std::vector<file_mapping_result> map_piece_to_files(
			piece_index_t piece, int offset, int length, bool write_mode
			, storage_error& ec);

		bool has_any_file(storage_error& error);
		void set_file_priority(settings_interface const&
			, aux::vector<download_priority_t, file_index_t>& prio
			, storage_error& ec);
		bool verify_resume_data(add_torrent_params const& rd
			, aux::vector<std::string, file_index_t> const& links
			, storage_error& ec);

		void release_files();

		void delete_files(remove_flags_t options, storage_error& error);

		std::pair<status_t, std::string> move_storage(std::string const& sp
			, move_flags_t flags, storage_error& ec);

		void rename_file(file_index_t index, std::string const& new_filename
			, storage_error& ec);

		status_t initialize(settings_interface const&, storage_error& ec);

	private:

		int open_file_impl(file_index_t idx, int flags, storage_error& ec);

		void need_partfile();
		bool use_partfile(file_index_t index) const;
		void use_partfile(file_index_t index, bool b);

		file_storage const& m_files;
		std::unique_ptr<file_storage> m_mapped_files;
		std::string m_save_path;
		stat_cache m_stat_cache;

		aux::vector<download_priority_t, file_index_t> m_file_priority;
		aux::vector<bool, file_index_t> m_use_partfile;

		std::string m_part_file_name;
		std::unique_ptr<posix_part_file> m_part_file;

		// cache of open file descriptors
		std::unordered_map<int, int> m_open_files; // file_index -> fd
	};

} // namespace aux
} // namespace libtorrent

#endif // TORRENT_HAVE_IO_URING

#endif // TORRENT_IO_URING_STORAGE_HPP
