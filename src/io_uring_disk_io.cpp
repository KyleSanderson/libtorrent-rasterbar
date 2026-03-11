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

// ============================================================
// io_uring disk backend
// ============================================================
//
// Architecture
// ------------
// A single dedicated disk thread owns ALL storage objects and the io_uring
// ring.  The network / calling thread only enqueues jobs (std::function<void()>)
// and returns immediately.
//
// The disk thread runs a two-phase inner loop:
//   Phase 1 – Job intake:  drain the pending queue, open files, and for
//              every read/write prepare one io_uring SQE.  Other (meta)
//              operations are run synchronously inline.
//   Phase 2 – CQE harvest:  after submitting all SQEs call
//              io_uring_submit_and_wait(1) then drain every ready CQE.
//              For each CQE call the stored callback, which posts the
//              result back to m_ios (the network thread's io_context).
//
// This design gives:
//   • No blocking calls on the network thread.
//   • io_uring batching: multiple I/O operations are inflight concurrently.
//   • Single-threaded storage access (no locks on storage objects).
//   • Clean shutdown: disk thread drains outstanding CQEs before joining.
//   • Correct write-buffer lifetime: the kernel reads from the caller's
//     buffer inside the disk thread; the callback is only posted *after*
//     the CQE arrives, so the caller cannot free the buffer prematurely.
//
// Fallback
// --------
// If io_uring fails to initialise (old kernel, RLIMIT_MEMLOCK, …) every
// read/write falls back to pread/pwrite synchronously inside the disk
// thread.  Everything else is unaffected.

#include "libtorrent/config.hpp"

#if TORRENT_HAVE_IO_URING

#include "libtorrent/io_uring_disk_io.hpp"
#include "libtorrent/aux_/io_uring_storage.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/aux_/disk_buffer_pool.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/performance_counters.hpp"
#include "libtorrent/aux_/session_settings.hpp"
#include "libtorrent/aux_/path.hpp"
#include "libtorrent/aux_/numeric_cast.hpp"
#include "libtorrent/stat_cache.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/aux_/storage_free_list.hpp"
#include "libtorrent/settings_pack.hpp"
#include "libtorrent/error.hpp"

#include <liburing.h>

#include <vector>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <memory>
#include <cassert>

namespace libtorrent {

namespace {

	using aux::io_uring_storage;

	// Round v up to the nearest power of two.
	unsigned next_pow2(unsigned v)
	{
		if (v <= 1) return 1;
		v--;
		v |= v >> 1; v |= v >> 2; v |= v >> 4; v |= v >> 8; v |= v >> 16;
		return v + 1;
	}

	// Compute a ring depth scaled to hardware concurrency, clamped and
	// rounded to the next power of two.
	unsigned compute_ring_depth(unsigned per_core, unsigned floor_depth, unsigned ceil_depth)
	{
		unsigned const cores = std::max(1u
			, static_cast<unsigned>(std::thread::hardware_concurrency()));
		unsigned const n = std::max(floor_depth
			, std::min(ceil_depth, cores * per_core));
		return next_pow2(n);
	}

	// Maximum number of SQEs we submit in one batch before waiting for CQEs.
	// Set at construction based on the ring depth (half the ring for headroom).

	// ---------------------------------------------------------------
	// Per-SQE completion callback stored as the user_data pointer.
	// Called by the disk thread with the CQE result code.
	// ---------------------------------------------------------------
	struct sqe_cb
	{
		std::function<void(int /*cqe_res*/)> fn;
	};

} // anonymous namespace

// ================================================================
// io_uring_disk_io
// ================================================================
struct TORRENT_EXTRA_EXPORT io_uring_disk_io final
	: disk_interface
{
	// ------------------------------------------------------------------
	// Construction / destruction
	// ------------------------------------------------------------------
	io_uring_disk_io(io_context& ios
		, settings_interface const& sett
		, counters& cnt)
		: m_settings(sett)
		, m_buffer_pool(ios)
		, m_stats_counters(cnt)
		, m_ios(ios)
		, m_ring_depth(compute_ring_depth(64u, 256u, 16384u))
		, m_max_batch_sqes(m_ring_depth / 2)
	{
		settings_updated();

		int const rc = io_uring_queue_init(m_ring_depth, &m_ring, 0);
		m_uring_ok = (rc == 0);

		m_disk_thread = std::thread([this] { disk_thread_fn(); });
	}

	~io_uring_disk_io() override
	{
		// abort(true) joins the disk thread; only then safe to exit io_uring.
		abort(true);
		if (m_uring_ok) io_uring_queue_exit(&m_ring);
	}

	void settings_updated() override
	{
		m_buffer_pool.set_settings(m_settings);
	}

	// ------------------------------------------------------------------
	// Storage management (called from network thread, never touch storage
	// objects here – they are owned by the disk thread)
	// ------------------------------------------------------------------
	storage_holder new_torrent(storage_params const& params
		, std::shared_ptr<void> const&) override
	{
		// We must allocate the index on the calling thread to make the
		// storage_holder valid immediately.  The storage object itself is
		// created inline here; all subsequent access is from the disk thread.
		std::lock_guard<std::mutex> lg(m_storage_mutex);
		storage_index_t const idx = m_free_slots.new_index(m_torrents.end_index());
		auto storage = std::make_unique<io_uring_storage>(params);
		if (idx == m_torrents.end_index()) m_torrents.emplace_back(std::move(storage));
		else m_torrents[idx] = std::move(storage);
		return storage_holder(idx, *this);
	}

	void remove_torrent(storage_index_t const idx) override
	{
		// run in disk thread so any in-flight jobs finish first
		push_job([this, idx]
		{
			std::lock_guard<std::mutex> lg(m_storage_mutex);
			m_torrents[idx].reset();
			m_free_slots.add(idx);
		});
	}

	// ------------------------------------------------------------------
	// Lifecycle
	// ------------------------------------------------------------------
	void abort(bool wait) override
	{
		m_abort.store(true, std::memory_order_release);
		m_cv.notify_all();
		if (wait && m_disk_thread.joinable())
			m_disk_thread.join();
	}

	// ------------------------------------------------------------------
	// Async I/O
	// ------------------------------------------------------------------
	void async_read(storage_index_t storage, peer_request const& r
		, std::function<void(disk_buffer_holder, storage_error const&)> handler
		, disk_job_flags_t flags) override
	{
		// Allocate the destination buffer on the calling thread.
		disk_buffer_holder buffer(m_buffer_pool
			, m_buffer_pool.allocate_buffer("send buffer"), default_block_size);
		if (!buffer)
		{
			storage_error err;
			err.ec        = errors::no_memory;
			err.operation = operation_t::alloc_cache_piece;
			post(m_ios, [this, err, h = std::move(handler)] {
				h(disk_buffer_holder(m_buffer_pool, nullptr, 0), err);
			});
			return;
		}

		// Transfer ownership of the buffer into a shared_ptr so the lambda can
		// hold it alive until the CQE arrives and the callback fires.
		auto buf_holder = std::make_shared<disk_buffer_holder>(std::move(buffer));
		auto start_time = clock_type::now();

		push_job([this, storage, r, flags, buf_holder, start_time
				, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;

			storage_error error;
			span<char> buf{buf_holder->data(), r.length};

			// Try io_uring fast path (single file mapping, no pad files, no partfile).
			bool submitted = false;
			if (m_uring_ok)
			{
				auto mappings = st->map_piece_to_files(r.piece, r.start
					, r.length, false, error);
				if (!error.ec && mappings.size() == 1
					&& !mappings[0].is_pad_file
					&& !mappings[0].uses_partfile)
				{
					auto& m = mappings[0];
					struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
					if (sqe)
					{
						io_uring_prep_read(sqe, m.fd, buf_holder->data()
							, static_cast<unsigned>(m.length)
							, static_cast<unsigned long long>(m.file_offset));

						auto* cb = new sqe_cb{[this, buf_holder, start_time
								, h = std::move(h), r](int res) mutable
						{
							storage_error e;
							if (res < 0)
							{
								e.ec.assign(-res, generic_category());
								e.operation = operation_t::file_read;
								post(m_ios, [this, e, h = std::move(h)] {
									h(disk_buffer_holder(m_buffer_pool, nullptr, 0), e);
								});
								return;
							}
							auto dur = total_microseconds(clock_type::now() - start_time);
							m_stats_counters.inc_stats_counter(counters::num_blocks_read);
							m_stats_counters.inc_stats_counter(counters::num_read_ops);
							m_stats_counters.inc_stats_counter(counters::disk_read_time, dur);
							m_stats_counters.inc_stats_counter(counters::disk_job_time, dur);
							post(m_ios
							    , [h = std::move(h), bh = std::move(buf_holder)]() mutable {
								h(std::move(*bh), storage_error());
							});
						}};
						io_uring_sqe_set_data(sqe, cb);
						++m_pending_sqes;
						submitted = true;
					}
				}
			}

			if (!submitted)
			{
				// Synchronous fallback (pread through storage's readwrite helper).
				storage_error serr;
				st->read(m_settings, buf, r.piece, r.start, serr);
				if (serr.ec) {}
				else
				{
					auto dur = total_microseconds(clock_type::now() - start_time);
					m_stats_counters.inc_stats_counter(counters::num_blocks_read);
					m_stats_counters.inc_stats_counter(counters::num_read_ops);
					m_stats_counters.inc_stats_counter(counters::disk_read_time, dur);
					m_stats_counters.inc_stats_counter(counters::disk_job_time, dur);
				}
				post(m_ios, [h = std::move(h), bh = std::move(buf_holder), serr]() mutable {
					h(std::move(*bh), serr);
				});
			}
		});
	}

	bool async_write(storage_index_t storage, peer_request const& r
		, char const* buf, std::shared_ptr<disk_observer>
		, std::function<void(storage_error const&)> handler
		, disk_job_flags_t flags) override
	{
		auto start_time = clock_type::now();

		// Copy the caller's buffer into a pool buffer so we own it until the
		// CQE fires, even though the caller is free to return.
		disk_buffer_holder copy_buf(m_buffer_pool
			, m_buffer_pool.allocate_buffer("write buffer"), default_block_size);
		if (!copy_buf)
		{
			storage_error err;
			err.ec        = errors::no_memory;
			err.operation = operation_t::alloc_cache_piece;
			post(m_ios, [err, h = std::move(handler)] { h(err); });
			return false;
		}
		std::memcpy(copy_buf.data(), buf, static_cast<std::size_t>(r.length));

		auto buf_holder = std::make_shared<disk_buffer_holder>(std::move(copy_buf));

		push_job([this, storage, r, flags, buf_holder, start_time
				, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;

			storage_error error;
			bool submitted = false;
			if (m_uring_ok)
			{
				auto mappings = st->map_piece_to_files(r.piece, r.start
					, r.length, true, error);
				if (!error.ec && mappings.size() == 1
					&& !mappings[0].is_pad_file
					&& !mappings[0].uses_partfile)
				{
					auto& m = mappings[0];
					struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
					if (sqe)
					{
						io_uring_prep_write(sqe, m.fd, buf_holder->data()
							, static_cast<unsigned>(m.length)
							, static_cast<unsigned long long>(m.file_offset));

						auto* cb = new sqe_cb{[this, buf_holder, start_time
								, h = std::move(h)](int res) mutable
						{
							// buf_holder keeps write buffer alive until here
							buf_holder.reset();
							storage_error e;
							if (res < 0)
							{
								e.ec.assign(-res, generic_category());
								e.operation = operation_t::file_write;
								post(m_ios, [e, h = std::move(h)] { h(e); });
								return;
							}
							auto dur = total_microseconds(clock_type::now() - start_time);
							m_stats_counters.inc_stats_counter(counters::num_blocks_written);
							m_stats_counters.inc_stats_counter(counters::num_write_ops);
							m_stats_counters.inc_stats_counter(counters::disk_write_time, dur);
							m_stats_counters.inc_stats_counter(counters::disk_job_time, dur);
							post(m_ios, [h = std::move(h)] { h(storage_error()); });
						}};
						io_uring_sqe_set_data(sqe, cb);
						++m_pending_sqes;
						submitted = true;
					}
				}
			}

			if (!submitted)
			{
				span<char> b{buf_holder->data(), r.length};
				storage_error serr;
				st->write(m_settings, b, r.piece, r.start, serr);
				if (serr.ec) {}
				buf_holder.reset();
				if (!serr.ec)
				{
					auto dur = total_microseconds(clock_type::now() - start_time);
					m_stats_counters.inc_stats_counter(counters::num_blocks_written);
					m_stats_counters.inc_stats_counter(counters::num_write_ops);
					m_stats_counters.inc_stats_counter(counters::disk_write_time, dur);
					m_stats_counters.inc_stats_counter(counters::disk_job_time, dur);
				}
				post(m_ios, [serr, h = std::move(h)] { h(serr); });
			}
		});
		return false;
	}

	// ------------------------------------------------------------------
	// Hash
	// ------------------------------------------------------------------
	void async_hash(storage_index_t storage, piece_index_t const piece
		, span<sha256_hash> block_hashes, disk_job_flags_t flags
		, std::function<void(piece_index_t, sha1_hash const&
			, storage_error const&)> handler) override
	{
		// IMPORTANT: block_hashes is a span pointing into the caller's vector
		// (which lives inside the handler closure). We must write v2 hashes
		// directly into this span, NOT into a copy, because the caller reads
		// the results from there after the handler fires.
		push_job([this, storage, piece, flags, block_hashes
				, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;

			auto start_time = clock_type::now();

			bool const v1 = bool(flags & disk_interface::v1_hash);
			bool const v2 = !block_hashes.empty();

			disk_buffer_holder buffer(m_buffer_pool
				, m_buffer_pool.allocate_buffer("hash buffer"), default_block_size);
			storage_error error;
			if (!buffer)
			{
				error.ec        = errors::no_memory;
				error.operation = operation_t::alloc_cache_piece;
				post(m_ios, [piece, error, h = std::move(h)] {
					h(piece, sha1_hash{}, error);
				});
				return;
			}
			hasher ph;

			int const piece_size       = v1 ? st->files().piece_size(piece)  : 0;
			int const piece_size2      = v2 ? st->files().piece_size2(piece) : 0;
			int const blocks_in_piece  = v1
				? (piece_size  + default_block_size - 1) / default_block_size : 0;
			int const blocks_in_piece2 = v2
				? st->files().blocks_in_piece2(piece) : 0;

			int offset          = 0;
			int const blocks_n  = std::max(blocks_in_piece, blocks_in_piece2);
			for (int i = 0; i < blocks_n; ++i)
			{
				bool const v2_block = i < blocks_in_piece2;
				auto const len  = v1 ? std::min(default_block_size, piece_size  - offset) : 0;
				auto const len2 = v2_block
					? std::min(default_block_size, piece_size2 - offset) : 0;

				span<char> const b{buffer.data(), std::max(len, len2)};
				int const ret = st->read(m_settings, b, piece, offset, error);
				offset += default_block_size;
				if (ret <= 0) break;
				if (v1)                ph.update(b.first(std::min(ret, len)));
				if (v2_block) block_hashes[i] = hasher256(b.first(std::min(ret, len2))).final();
			}

			sha1_hash const hash = v1 ? ph.final() : sha1_hash();
			if (!error.ec)
			{
				auto dur = total_microseconds(clock_type::now() - start_time);
				m_stats_counters.inc_stats_counter(counters::num_read_back, blocks_n);
				m_stats_counters.inc_stats_counter(counters::num_blocks_read, blocks_n);
				m_stats_counters.inc_stats_counter(counters::num_read_ops, blocks_n);
				m_stats_counters.inc_stats_counter(counters::disk_hash_time, dur);
				m_stats_counters.inc_stats_counter(counters::disk_job_time, dur);
			}
			post(m_ios, [piece, hash, error, h = std::move(h)] {
				h(piece, hash, error);
			});
		});
	}

	void async_hash2(storage_index_t storage, piece_index_t const piece
		, int offset, disk_job_flags_t
		, std::function<void(piece_index_t, sha256_hash const&
			, storage_error const&)> handler) override
	{
		push_job([this, storage, piece, offset, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;

			auto start_time = clock_type::now();
			disk_buffer_holder buffer(m_buffer_pool
				, m_buffer_pool.allocate_buffer("hash buffer"), default_block_size);
			storage_error error;
			if (!buffer)
			{
				error.ec        = errors::no_memory;
				error.operation = operation_t::alloc_cache_piece;
				post(m_ios, [piece, error, h = std::move(h)] {
					h(piece, sha256_hash{}, error);
				});
				return;
			}

			int const piece_size = st->files().piece_size2(piece);
			auto const len       = std::min(default_block_size, piece_size - offset);

			hasher256 ph;
			span<char> const b{buffer.data(), len};
			int const ret = st->read(m_settings, b, piece, offset, error);
			if (ret > 0) ph.update(b.first(ret));
			sha256_hash const hash = ph.final();

			if (!error.ec)
			{
				auto dur = total_microseconds(clock_type::now() - start_time);
				m_stats_counters.inc_stats_counter(counters::num_read_back);
				m_stats_counters.inc_stats_counter(counters::num_blocks_read);
				m_stats_counters.inc_stats_counter(counters::num_read_ops);
				m_stats_counters.inc_stats_counter(counters::disk_hash_time, dur);
				m_stats_counters.inc_stats_counter(counters::disk_job_time, dur);
			}
			post(m_ios, [piece, hash, error, h = std::move(h)] {
				h(piece, hash, error);
			});
		});
	}

	// ------------------------------------------------------------------
	// Meta-ops (all run in the disk thread)
	// ------------------------------------------------------------------
	void async_move_storage(storage_index_t const storage, std::string p
		, move_flags_t const flags
		, std::function<void(status_t, std::string const&
			, storage_error const&)> handler) override
	{
		push_job([this, storage, p = std::move(p), flags, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;
			storage_error ec;
			status_t ret;
			std::tie(ret, p) = st->move_storage(p, flags, ec);
			post(m_ios, [ret, p = std::move(p), ec, h = std::move(h)]() mutable {
				h(ret, std::move(p), ec);
			});
		});
	}

	void async_release_files(storage_index_t storage
		, std::function<void()> handler) override
	{
		push_job([this, storage, h = std::move(handler)]
		{
			io_uring_storage* st = get_storage(storage);
			if (st) st->release_files();
			if (h) post(m_ios, h);
		});
	}

	void async_delete_files(storage_index_t storage, remove_flags_t options
		, std::function<void(storage_error const&)> handler) override
	{
		push_job([this, storage, options, h = std::move(handler)]
		{
			io_uring_storage* st = get_storage(storage);
			storage_error error;
			if (st) st->delete_files(options, error);
			post(m_ios, [error, h = std::move(h)] { h(error); });
		});
	}

	void async_check_files(storage_index_t storage
		, add_torrent_params const* resume_data
		, aux::vector<std::string, file_index_t> links
		, std::function<void(status_t, storage_error const&)> handler) override
	{
		// resume_data may not outlive this call; copy it if present.
		std::shared_ptr<add_torrent_params> rd_copy;
		if (resume_data)
			rd_copy = std::make_shared<add_torrent_params>(*resume_data);

		push_job([this, storage, rd_copy, links = std::move(links)
				, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;

			add_torrent_params empty;
			add_torrent_params const* rd = rd_copy ? rd_copy.get() : &empty;

			storage_error error;
			status_t const ret = [&]
			{
				auto const ret_flag = st->initialize(m_settings, error);
				if (error) return status_t::fatal_disk_error | ret_flag;

				bool const ok = st->verify_resume_data(*rd, std::move(links), error);

				if (m_settings.get_bool(settings_pack::no_recheck_incomplete_resume))
					return status_t::no_error | ret_flag;

				if (!aux::contains_resume_data(*rd))
				{
					storage_error ignore;
					return (st->has_any_file(ignore)
						? status_t::need_full_check
						: status_t::no_error)
						| ret_flag;
				}
				return (ok
					? status_t::no_error
					: status_t::need_full_check)
					| ret_flag;
			}();

			post(m_ios, [error, ret, h = std::move(h)] { h(ret, error); });
		});
	}

	void async_rename_file(storage_index_t storage, file_index_t idx
		, std::string name
		, std::function<void(std::string const&, file_index_t
			, storage_error const&)> handler) override
	{
		push_job([this, storage, idx, name = std::move(name)
				, h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;
			storage_error error;
			st->rename_file(idx, name, error);
			post(m_ios, [idx, name = std::move(name), error, h = std::move(h)]() mutable {
				h(std::move(name), idx, error);
			});
		});
	}

	void async_stop_torrent(storage_index_t, std::function<void()> handler) override
	{
		if (!handler) return;
		post(m_ios, std::move(handler));
	}

	void async_set_file_priority(storage_index_t storage
		, aux::vector<download_priority_t, file_index_t> prio
		, std::function<void(storage_error const&
			, aux::vector<download_priority_t, file_index_t>)> handler) override
	{
		push_job([this, storage, prio = std::move(prio), h = std::move(handler)]() mutable
		{
			io_uring_storage* st = get_storage(storage);
			if (!st) return;
			storage_error error;
			st->set_file_priority(m_settings, prio, error);
			post(m_ios, [p = std::move(prio), error, h = std::move(h)]() mutable {
				h(error, std::move(p));
			});
		});
	}

	void async_clear_piece(storage_index_t, piece_index_t index
		, std::function<void(piece_index_t)> handler) override
	{
		post(m_ios, [=, h = std::move(handler)] { h(index); });
	}

	void update_stats_counters(counters&) const override {}

	std::vector<open_file_state> get_status(storage_index_t) const override
	{ return {}; }

	void submit_jobs() override {}

private:

	// ------------------------------------------------------------------
	// Disk thread implementation
	// ------------------------------------------------------------------

	void push_job(std::function<void()> job)
	{
		{
			std::lock_guard<std::mutex> lg(m_queue_mutex);
			m_jobs.push_back(std::move(job));
		}
		m_cv.notify_one();
	}

	// Returns the storage pointer under m_storage_mutex.  Returns nullptr
	// if the storage has already been removed.
	io_uring_storage* get_storage(storage_index_t idx)
	{
		std::lock_guard<std::mutex> lg(m_storage_mutex);
		if (idx >= m_torrents.end_index()) return nullptr;
		return m_torrents[idx].get();
	}

	// Harvest all ready CQEs and dispatch their callbacks (disk thread only).
	void drain_cqes()
	{
		if (!m_uring_ok || m_pending_sqes == 0) return;

		struct io_uring_cqe* cqe = nullptr;
		while (m_pending_sqes > 0
			&& io_uring_peek_cqe(&m_ring, &cqe) == 0
			&& cqe != nullptr)
		{
			auto* cb = static_cast<sqe_cb*>(io_uring_cqe_get_data(cqe));
			if (cb) { cb->fn(cqe->res); delete cb; }
			io_uring_cqe_seen(&m_ring, cqe);
			--m_pending_sqes;
			cqe = nullptr;
		}
	}

	// Submit any queued SQEs and wait for at least one CQE.
	// Used when we have outstanding async ops but the job queue is empty.
	void wait_for_cqes()
	{
		if (!m_uring_ok || m_pending_sqes == 0) return;

		// Submit everything queued so far, then wait for ≥1 CQE.
		io_uring_submit_and_wait(&m_ring, 1);
		drain_cqes();
	}

	void disk_thread_fn()
	{
		while (true)
		{
			// ---- Phase 1: grab all pending jobs ----
			std::vector<std::function<void()>> batch;
			{
				std::unique_lock<std::mutex> lk(m_queue_mutex);
				m_cv.wait(lk, [this] {
					return m_abort.load(std::memory_order_relaxed)
					    || !m_jobs.empty();
				});

				if (!m_jobs.empty())
				{
		// Drain at most m_max_batch_sqes jobs at once.
					unsigned const take = std::min<unsigned>(
						static_cast<unsigned>(m_jobs.size()), m_max_batch_sqes);
					batch.reserve(take);
					for (unsigned i = 0; i < take; ++i)
					{
						batch.push_back(std::move(m_jobs.front()));
						m_jobs.pop_front();
					}
				}
			}

			// Abort: drain remaining jobs so callers get their callbacks.
			if (m_abort.load(std::memory_order_relaxed) && batch.empty())
			{
				// Flush outstanding CQEs first.
				if (m_uring_ok && m_pending_sqes > 0)
				{
					io_uring_submit(&m_ring);
					while (m_pending_sqes > 0)
					{
						struct io_uring_cqe* cqe = nullptr;
						if (io_uring_wait_cqe(&m_ring, &cqe) < 0) break;
						auto* cb = static_cast<sqe_cb*>(io_uring_cqe_get_data(cqe));
						if (cb) { cb->fn(cqe->res); delete cb; }
						io_uring_cqe_seen(&m_ring, cqe);
						--m_pending_sqes;
					}
				}
				break;
			}

			// ---- Phase 2: execute jobs (may submit SQEs) ----
			for (auto& job : batch)
				job();

			// ---- Phase 3: submit all queued SQEs and harvest CQEs ----
			if (m_uring_ok && m_pending_sqes > 0)
			{
				// Submit then wait for ALL outstanding CQEs so we never sleep
				// with in-flight I/O.  This keeps latency predictable and
				// avoids the condvar missing the "work done" event.
				io_uring_submit(&m_ring);
				while (m_pending_sqes > 0)
				{
					struct io_uring_cqe* cqe = nullptr;
					if (io_uring_wait_cqe(&m_ring, &cqe) < 0) break;
					auto* cb = static_cast<sqe_cb*>(io_uring_cqe_get_data(cqe));
					if (cb) { cb->fn(cqe->res); delete cb; }
					io_uring_cqe_seen(&m_ring, cqe);
					--m_pending_sqes;
				}
			}
		}
	}

	// ------------------------------------------------------------------
	// Members
	// ------------------------------------------------------------------

	// Storage objects – accessed under m_storage_mutex.
	// All disk-thread jobs obtain the raw pointer first then release the lock,
	// which is safe because remove_torrent runs inside the disk thread too.
	aux::vector<std::unique_ptr<io_uring_storage>, storage_index_t> m_torrents;
	aux::storage_free_list  m_free_slots;
	mutable std::mutex      m_storage_mutex;

	settings_interface const& m_settings;
	aux::disk_buffer_pool   m_buffer_pool;
	counters&               m_stats_counters;
	io_context&             m_ios;

	// io_uring (accessed only from disk thread after init)
	struct io_uring  m_ring{};
	unsigned         m_ring_depth    = 0;
	unsigned         m_max_batch_sqes = 0;
	bool             m_uring_ok   = false;
	unsigned         m_pending_sqes = 0;  // SQEs in flight (disk thread only)

	// Job queue
	std::deque<std::function<void()>> m_jobs;
	std::mutex                        m_queue_mutex;
	std::condition_variable           m_cv;

	std::atomic<bool>  m_abort{false};
	std::thread        m_disk_thread;
};

TORRENT_EXPORT std::unique_ptr<disk_interface> io_uring_disk_io_constructor(
	io_context& ios, settings_interface const& sett, counters& cnt)
{
	return std::make_unique<io_uring_disk_io>(ios, sett, cnt);
}

} // namespace libtorrent

#endif // TORRENT_HAVE_IO_URING
