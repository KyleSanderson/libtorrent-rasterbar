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

#ifndef TORRENT_IO_URING_SOCKET_HPP
#define TORRENT_IO_URING_SOCKET_HPP

#include "libtorrent/config.hpp"

#if TORRENT_HAVE_IO_URING

#include "libtorrent/io_context.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/span.hpp"

#include <liburing.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <functional>
#include <mutex>
#include <thread>
#include <atomic>
#include <deque>
#include <memory>

namespace libtorrent {
namespace aux {

	// io_uring_event_loop provides a bridge between io_uring and boost::asio.
	// It runs a dedicated thread that reaps io_uring CQEs and posts completion
	// handlers back onto boost::asio's io_context.
	//
	// One-shot ops (recv, send, poll_read, poll_write, …):
	//   Submit one SQE, receive exactly one CQE, then free state.
	//
	// Multishot ops (accept_multishot, recv_multishot, recvmsg_multishot,
	//   poll_multishot):
	//   Submit one SQE, receive multiple CQEs until the kernel clears
	//   IORING_CQE_F_MORE (error, socket close, etc). Handlers receive a
	//   `bool more` argument; when `more==false` the multishot has ended and
	//   the caller must re-submit if another round is needed.
	//   Call cancel() with the returned token to stop a running multishot.
	struct TORRENT_EXTRA_EXPORT io_uring_event_loop
	{
		explicit io_uring_event_loop(io_context& ios);
		~io_uring_event_loop();

		io_uring_event_loop(io_uring_event_loop const&) = delete;
		io_uring_event_loop& operator=(io_uring_event_loop const&) = delete;

		bool is_initialized() const { return m_initialized; }

		// Ring depth chosen at construction based on hardware concurrency.
		unsigned ring_depth() const { return m_ring_depth; }

		// Opaque token returned by multishot ops. Pass to cancel() to stop them.
		using cancel_token = void const*;
		static constexpr cancel_token no_token = nullptr;

		// ----------------------------------------------------------------
		// One-shot operations
		// ----------------------------------------------------------------

		void async_recv(int fd, span<char> buf
			, std::function<void(int, error_code)> handler);

		void async_send(int fd, span<char const> buf
			, std::function<void(int, error_code)> handler);

		void async_recvmsg(int fd, struct msghdr* msg
			, std::function<void(int, error_code)> handler);

		void async_sendmsg(int fd, struct msghdr const* msg
			, std::function<void(int, error_code)> handler);

		// poll_add: fires once when fd becomes readable/writable.
		void async_poll_read(int fd, std::function<void(error_code)> handler);
		void async_poll_write(int fd, std::function<void(error_code)> handler);

		// ----------------------------------------------------------------
		// Multishot operations
		// ----------------------------------------------------------------

		// Accept connections repeatedly from a listening socket.
		// handler(new_fd, peer_addr, addrlen, ec, more)
		//   new_fd >= 0 on success. more==false means the multishot ended.
		// Returns a cancel_token that can be passed to cancel().
		cancel_token async_accept_multishot(int fd
			, struct sockaddr* addr, socklen_t* addrlen
			, std::function<void(int, struct sockaddr*, socklen_t, error_code, bool)> handler);

		// Receive data repeatedly from a stream socket.
		// handler(bytes, ec, more)
		cancel_token async_recv_multishot(int fd, span<char> buf
			, std::function<void(int, error_code, bool)> handler);

		// Receive datagrams repeatedly (UDP).
		// handler(bytes, ec, more)
		cancel_token async_recvmsg_multishot(int fd, struct msghdr* msg
			, std::function<void(int, error_code, bool)> handler);

		// Poll for readability continuously (re-arms after each event).
		// handler(ec, more)
		cancel_token async_poll_multishot(int fd, unsigned events
			, std::function<void(error_code, bool)> handler);

		// Cancel a running multishot op. The final CQE posted by the kernel
		// (with IORING_CQE_F_MORE clear) will still arrive and call the handler
		// with ECANCELED.
		void cancel(cancel_token token);

		// flush pending SQEs
		void submit();

		// shutdown the event loop
		void stop();

	private:
		enum class net_op_type : std::uint8_t
		{
			// one-shot
			recv, send, recvmsg, sendmsg, poll_read, poll_write,
			// multishot
			accept_ms, recv_ms, recvmsg_ms, poll_ms,
			// internal
			nop
		};

		struct net_completion
		{
			net_op_type op;
			bool multishot = false;

			// one-shot handlers
			std::function<void(int, error_code)>  handler_2arg;
			std::function<void(error_code)>       handler_1arg;

			// multishot handlers (only one of these will be set per instance)
			std::function<void(int, struct sockaddr*, socklen_t, error_code, bool)> handler_accept;
			std::function<void(int, error_code, bool)> handler_3arg;
			std::function<void(error_code, bool)>      handler_poll_ms;

			// for accept multishot: addr storage lives in the caller
			struct sockaddr* addr    = nullptr;
			socklen_t*       addrlen = nullptr;

			// for recv_ms / recvmsg_ms re-queue on completion
			int          requeue_fd  = -1;
			char*        recv_buf    = nullptr;
			int          recv_len    = 0;
			struct msghdr* rmsg_ptr  = nullptr;
		};

		// Overflow queue for one-shot ops that couldn't be staged immediately.
		// Multishot ops are never queued here — they are submitted from the
		// caller thread once an SQE slot opens (or fail with ECANCELED).
		struct pending_submission
		{
			net_completion* ctx;
			int             fd;
			union {
				struct recv_buf_t { char*        ptr; int len; } recv_buf;
				struct send_buf_t { char const*  ptr; int len; } send_buf;
				::msghdr*                                        rmsg;
				::msghdr const*                                  wmsg;
			} u;
		};

		// Submit helper: get a fresh SQE, flushing the ring if needed.
		// Returns nullptr only if ring is genuinely unable to give a slot.
		struct io_uring_sqe* get_sqe_unlocked();

		void reaper_thread_fn();
		void flush_one_overflow(struct io_uring_sqe* sqe);
		void dispatch_cqe(net_completion* ctx, int res, unsigned flags);

		io_context& m_ios;
		struct io_uring m_ring;
		unsigned m_ring_depth = 0;
		bool m_initialized = false;
		std::atomic<bool> m_abort{false};
		std::mutex m_ring_mutex;
		std::deque<pending_submission> m_overflow; // guarded by m_ring_mutex
		std::thread m_reaper;
	};

} // namespace aux
} // namespace libtorrent

#endif // TORRENT_HAVE_IO_URING
#endif // TORRENT_IO_URING_SOCKET_HPP
