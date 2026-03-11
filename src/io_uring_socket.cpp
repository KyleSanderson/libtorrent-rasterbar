#include "libtorrent/config.hpp"

#if TORRENT_HAVE_IO_URING

#include "libtorrent/aux_/io_uring_socket.hpp"

#include <poll.h>
#include <cerrno>
#include <cstring>

namespace libtorrent {
namespace aux {

namespace {

	unsigned next_pow2(unsigned v)
	{
		if (v <= 1) return 1;
		v--;
		v |= v >> 1; v |= v >> 2; v |= v >> 4; v |= v >> 8; v |= v >> 16;
		return v + 1;
	}

	unsigned compute_ring_depth(unsigned per_core, unsigned floor_depth, unsigned ceil_depth)
	{
		unsigned const cores = std::max(1u
			, static_cast<unsigned>(std::thread::hardware_concurrency()));
		unsigned const n = std::max(floor_depth
			, std::min(ceil_depth, cores * per_core));
		return next_pow2(n);
	}

} // anonymous namespace

// ============================================================
// Construction / destruction / stop
// ============================================================

io_uring_event_loop::io_uring_event_loop(io_context& ios)
	: m_ios(ios)
{
	m_ring_depth = compute_ring_depth(128u, 512u, 65536u);
	int ret = io_uring_queue_init(m_ring_depth, &m_ring, 0);
	if (ret < 0)
	{
		m_initialized = false;
		return;
	}
	m_initialized = true;
	m_reaper = std::thread([this] { reaper_thread_fn(); });
}

io_uring_event_loop::~io_uring_event_loop()
{
	stop();
}

void io_uring_event_loop::stop()
{
	bool expected = false;
	if (!m_abort.compare_exchange_strong(expected, true))
	{
		if (m_reaper.joinable()) m_reaper.join();
		return;
	}

	if (m_initialized)
	{
		// Submit a NOP to wake the blocked io_uring_wait_cqe in the reaper.
		std::lock_guard<std::mutex> l(m_ring_mutex);
		struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
		if (sqe)
		{
			io_uring_prep_nop(sqe);
			io_uring_sqe_set_data(sqe, nullptr);
			io_uring_submit(&m_ring);
		}
	}

	if (m_reaper.joinable()) m_reaper.join();

	if (m_initialized)
	{
		io_uring_queue_exit(&m_ring);
		m_initialized = false;
	}
}

// ============================================================
// Internal helpers
// ============================================================

// Get a fresh SQE, flushing if the ring is full.
// Must be called with m_ring_mutex held or only from the disk thread.
struct io_uring_sqe* io_uring_event_loop::get_sqe_unlocked()
{
	struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
	if (!sqe)
	{
		io_uring_submit(&m_ring);
		sqe = io_uring_get_sqe(&m_ring);
	}
	return sqe; // nullptr = ring genuinely full after flush
}

// Common guard used at the top of every async_* method.
// Returns true if the call should be rejected (not initialised or aborting).
static bool reject_if_stopping(bool initialized, std::atomic<bool> const& abort
	, io_context& ios, std::function<void(int, error_code)>& h)
{
	if (!initialized || abort.load(std::memory_order_acquire))
	{
		error_code ec(ECANCELED, generic_category());
		post(ios, [h2 = std::move(h), ec] { h2(0, ec); });
		return true;
	}
	return false;
}
static bool reject_if_stopping_1arg(bool initialized, std::atomic<bool> const& abort
	, io_context& ios, std::function<void(error_code)>& h)
{
	if (!initialized || abort.load(std::memory_order_acquire))
	{
		error_code ec(ECANCELED, generic_category());
		post(ios, [h2 = std::move(h), ec] { h2(ec); });
		return true;
	}
	return false;
}

// Dispatch a completed CQE. Handles both one-shot and multishot lifetime.
// Caller must have already called io_uring_cqe_seen before calling this.
void io_uring_event_loop::dispatch_cqe(net_completion* ctx, int res, unsigned flags)
{
	bool const more = (flags & IORING_CQE_F_MORE) != 0;

	error_code ec;
	int bytes = 0;
	if (res < 0)
		ec.assign(-res, generic_category());
	else
		bytes = res;

	switch (ctx->op)
	{
	// ---- one-shot: 2-arg handler ----
	case net_op_type::recv:
	case net_op_type::send:
	case net_op_type::recvmsg:
	case net_op_type::sendmsg:
		if (ctx->handler_2arg)
			post(m_ios, [h = std::move(ctx->handler_2arg), bytes, ec] { h(bytes, ec); });
		delete ctx;
		break;

	// ---- one-shot: 1-arg handler ----
	case net_op_type::poll_read:
	case net_op_type::poll_write:
		if (ctx->handler_1arg)
			post(m_ios, [h = std::move(ctx->handler_1arg), ec] { h(ec); });
		delete ctx;
		break;

	// ---- multishot: accept (handler gets new_fd, addr, addrlen, ec, more) ----
	case net_op_type::accept_ms:
		if (ctx->handler_accept)
			post(m_ios, [h = ctx->handler_accept, bytes, addr = ctx->addr
					, addrlen = ctx->addrlen, ec, more]
			{
				h(bytes /* = new fd on success */, addr, addrlen ? *addrlen : 0, ec, more);
			});
		if (!more) delete ctx;
		break;

	// ---- multishot: recv / recvmsg (handler: bytes, ec, more)
	//
	// The kernel requires IOSQE_BUFFER_SELECT for true recv_multishot, which
	// in turn needs a registered provided-buffer ring.  Rather than requiring
	// callers to manage buffer rings, we emulate multishot semantics by simply
	// re-submitting the same SQE after each completion (manual multishot).
	// The handler still receives `more=true` after each byte-count CQE and
	// `more=false` only when we decide not to requeue (error / socket closed).
	case net_op_type::recv_ms:
	{
		bool const ok = (!ec && bytes > 0);
		if (ctx->handler_3arg)
			post(m_ios, [h = ctx->handler_3arg, bytes, ec, ok]
				{ h(bytes, ec, /*more=*/ok); });
		if (ok)
		{
			// Requeue the same ctx — reuse its fd + buffer fields.
			std::lock_guard<std::mutex> lk(m_ring_mutex);
			struct io_uring_sqe* s = get_sqe_unlocked();
			if (s)
			{
				io_uring_prep_recv(s, ctx->requeue_fd
					, ctx->recv_buf, ctx->recv_len, 0);
				io_uring_sqe_set_data(s, ctx);
				io_uring_submit(&m_ring);
				return; // ctx kept alive for next CQE
			}
			// Ring full: put back in overflow
			pending_submission ps{};
			ps.ctx        = ctx; ps.fd = ctx->requeue_fd;
			ps.u.recv_buf = {ctx->recv_buf, ctx->recv_len};
			m_overflow.push_back(ps);
			return; // ctx kept alive
		}
		// Error or EOF — multishot ends.
		delete ctx;
		break;
	}
	case net_op_type::recvmsg_ms:
	{
		bool const ok = (!ec && bytes > 0);
		if (ctx->handler_3arg)
			post(m_ios, [h = ctx->handler_3arg, bytes, ec, ok]
				{ h(bytes, ec, /*more=*/ok); });
		if (ok)
		{
			std::lock_guard<std::mutex> lk(m_ring_mutex);
			struct io_uring_sqe* s = get_sqe_unlocked();
			if (s)
			{
				io_uring_prep_recvmsg(s, ctx->requeue_fd, ctx->rmsg_ptr, 0);
				io_uring_sqe_set_data(s, ctx);
				io_uring_submit(&m_ring);
				return;
			}
			pending_submission ps{};
			ps.ctx    = ctx; ps.fd = ctx->requeue_fd; ps.u.rmsg = ctx->rmsg_ptr;
			m_overflow.push_back(ps);
			return;
		}
		delete ctx;
		break;
	}

	// ---- multishot: poll (handler: ec, more) ----
	case net_op_type::poll_ms:
		if (ctx->handler_poll_ms)
			post(m_ios, [h = ctx->handler_poll_ms, ec, more] { h(ec, more); });
		if (!more) delete ctx;
		break;

	default:
		delete ctx;
		break;
	}
}

// ============================================================
// One-shot operations
// ============================================================

void io_uring_event_loop::async_recv(int fd, span<char> buf
	, std::function<void(int, error_code)> handler)
{
	if (reject_if_stopping(m_initialized, m_abort, m_ios, handler)) return;

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::recv;
	ctx->handler_2arg = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		pending_submission ps{};
		ps.ctx        = ctx; ps.fd = fd;
		ps.u.recv_buf = {buf.data(), static_cast<int>(buf.size())};
		m_overflow.push_back(ps);
		return;
	}
	io_uring_prep_recv(sqe, fd, buf.data(), buf.size(), 0);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
}

void io_uring_event_loop::async_send(int fd, span<char const> buf
	, std::function<void(int, error_code)> handler)
{
	if (reject_if_stopping(m_initialized, m_abort, m_ios, handler)) return;

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::send;
	ctx->handler_2arg = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		pending_submission ps{};
		ps.ctx        = ctx; ps.fd = fd;
		ps.u.send_buf = {buf.data(), static_cast<int>(buf.size())};
		m_overflow.push_back(ps);
		return;
	}
	io_uring_prep_send(sqe, fd, buf.data(), buf.size(), MSG_NOSIGNAL);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
}

void io_uring_event_loop::async_recvmsg(int fd, struct msghdr* msg
	, std::function<void(int, error_code)> handler)
{
	if (reject_if_stopping(m_initialized, m_abort, m_ios, handler)) return;

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::recvmsg;
	ctx->handler_2arg = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		pending_submission ps{};
		ps.ctx    = ctx; ps.fd = fd; ps.u.rmsg = msg;
		m_overflow.push_back(ps);
		return;
	}
	io_uring_prep_recvmsg(sqe, fd, msg, 0);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
}

void io_uring_event_loop::async_sendmsg(int fd, struct msghdr const* msg
	, std::function<void(int, error_code)> handler)
{
	if (reject_if_stopping(m_initialized, m_abort, m_ios, handler)) return;

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::sendmsg;
	ctx->handler_2arg = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		pending_submission ps{};
		ps.ctx    = ctx; ps.fd = fd; ps.u.wmsg = msg;
		m_overflow.push_back(ps);
		return;
	}
	io_uring_prep_sendmsg(sqe, fd, msg, MSG_NOSIGNAL);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
}

void io_uring_event_loop::async_poll_read(int fd
	, std::function<void(error_code)> handler)
{
	if (reject_if_stopping_1arg(m_initialized, m_abort, m_ios, handler)) return;

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::poll_read;
	ctx->handler_1arg = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		pending_submission ps{};
		ps.ctx = ctx; ps.fd = fd;
		m_overflow.push_back(ps);
		return;
	}
	io_uring_prep_poll_add(sqe, fd, POLLIN);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
}

void io_uring_event_loop::async_poll_write(int fd
	, std::function<void(error_code)> handler)
{
	if (reject_if_stopping_1arg(m_initialized, m_abort, m_ios, handler)) return;

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::poll_write;
	ctx->handler_1arg = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		pending_submission ps{};
		ps.ctx = ctx; ps.fd = fd;
		m_overflow.push_back(ps);
		return;
	}
	io_uring_prep_poll_add(sqe, fd, POLLOUT);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
}

// ============================================================
// Multishot operations
// ============================================================

io_uring_event_loop::cancel_token
io_uring_event_loop::async_accept_multishot(int fd
	, struct sockaddr* addr, socklen_t* addrlen
	, std::function<void(int, struct sockaddr*, socklen_t, error_code, bool)> handler)
{
	if (!m_initialized || m_abort.load(std::memory_order_acquire))
	{
		error_code ec(ECANCELED, generic_category());
		socklen_t al = addrlen ? *addrlen : 0;
		post(m_ios, [h = std::move(handler), addr, al, ec]
			{ h(-1, addr, al, ec, false); });
		return no_token;
	}

	auto* ctx = new net_completion;
	ctx->op             = net_op_type::accept_ms;
	ctx->multishot      = true;
	ctx->handler_accept = std::move(handler);
	ctx->addr           = addr;
	ctx->addrlen        = addrlen;

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		// Multishot ops cannot go through the overflow queue (they need
		// their own SQE). Report failure immediately.
		error_code ec(ENOMEM, generic_category());
		auto h = std::move(ctx->handler_accept);
		delete ctx;
		socklen_t al = addrlen ? *addrlen : 0;
		post(m_ios, [h2 = std::move(h), addr, al, ec]
			{ h2(-1, addr, al, ec, false); });
		return no_token;
	}
	io_uring_prep_multishot_accept(sqe, fd, addr, addrlen, 0);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
	return static_cast<cancel_token>(ctx);
}

io_uring_event_loop::cancel_token
io_uring_event_loop::async_recv_multishot(int fd, span<char> buf
	, std::function<void(int, error_code, bool)> handler)
{
	if (!m_initialized || m_abort.load(std::memory_order_acquire))
	{
		error_code ec(ECANCELED, generic_category());
		post(m_ios, [h = std::move(handler), ec] { h(0, ec, false); });
		return no_token;
	}

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::recv_ms;
	ctx->multishot    = true;
	ctx->handler_3arg = std::move(handler);
	ctx->requeue_fd   = fd;
	ctx->recv_buf     = buf.data();
	ctx->recv_len     = static_cast<int>(buf.size());

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		error_code ec(ENOMEM, generic_category());
		auto h = std::move(ctx->handler_3arg);
		delete ctx;
		post(m_ios, [h2 = std::move(h), ec] { h2(0, ec, false); });
		return no_token;
	}
	// Use plain one-shot recv; dispatch_cqe will re-arm it after each CQE.
	io_uring_prep_recv(sqe, fd, buf.data(), buf.size(), 0);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
	return static_cast<cancel_token>(ctx);
}

io_uring_event_loop::cancel_token
io_uring_event_loop::async_recvmsg_multishot(int fd, struct msghdr* msg
	, std::function<void(int, error_code, bool)> handler)
{
	if (!m_initialized || m_abort.load(std::memory_order_acquire))
	{
		error_code ec(ECANCELED, generic_category());
		post(m_ios, [h = std::move(handler), ec] { h(0, ec, false); });
		return no_token;
	}

	auto* ctx = new net_completion;
	ctx->op           = net_op_type::recvmsg_ms;
	ctx->multishot    = true;
	ctx->handler_3arg = std::move(handler);
	ctx->requeue_fd   = fd;
	ctx->rmsg_ptr     = msg;

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		error_code ec(ENOMEM, generic_category());
		auto h = std::move(ctx->handler_3arg);
		delete ctx;
		post(m_ios, [h2 = std::move(h), ec] { h2(0, ec, false); });
		return no_token;
	}
	// Use plain one-shot recvmsg; dispatch_cqe re-arms after each CQE.
	io_uring_prep_recvmsg(sqe, fd, msg, 0);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
	return static_cast<cancel_token>(ctx);
}

io_uring_event_loop::cancel_token
io_uring_event_loop::async_poll_multishot(int fd, unsigned events
	, std::function<void(error_code, bool)> handler)
{
	if (!m_initialized || m_abort.load(std::memory_order_acquire))
	{
		error_code ec(ECANCELED, generic_category());
		post(m_ios, [h = std::move(handler), ec] { h(ec, false); });
		return no_token;
	}

	auto* ctx = new net_completion;
	ctx->op              = net_op_type::poll_ms;
	ctx->multishot       = true;
	ctx->handler_poll_ms = std::move(handler);

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe)
	{
		error_code ec(ENOMEM, generic_category());
		auto h = std::move(ctx->handler_poll_ms);
		delete ctx;
		post(m_ios, [h2 = std::move(h), ec] { h2(ec, false); });
		return no_token;
	}
	io_uring_prep_poll_multishot(sqe, fd, events);
	io_uring_sqe_set_data(sqe, ctx);
	io_uring_submit(&m_ring);
	return static_cast<cancel_token>(ctx);
}

void io_uring_event_loop::cancel(cancel_token token)
{
	if (!token || !m_initialized) return;

	std::lock_guard<std::mutex> l(m_ring_mutex);
	struct io_uring_sqe* sqe = get_sqe_unlocked();
	if (!sqe) return; // best-effort; caller can re-try
	io_uring_prep_cancel(sqe, const_cast<void*>(token), 0);
	io_uring_sqe_set_data(sqe, nullptr); // cancel CQE itself needs no handler
	io_uring_submit(&m_ring);
}

// ============================================================
// Overflow flush (one-shot ops only)
// ============================================================

void io_uring_event_loop::flush_one_overflow(struct io_uring_sqe* sqe)
{
	auto& p = m_overflow.front();
	switch (p.ctx->op)
	{
	case net_op_type::recv:
		io_uring_prep_recv(sqe, p.fd, p.u.recv_buf.ptr, p.u.recv_buf.len, 0);
		break;
	case net_op_type::send:
		io_uring_prep_send(sqe, p.fd, p.u.send_buf.ptr, p.u.send_buf.len, MSG_NOSIGNAL);
		break;
	case net_op_type::recvmsg:
		io_uring_prep_recvmsg(sqe, p.fd, p.u.rmsg, 0);
		break;
	case net_op_type::sendmsg:
		io_uring_prep_sendmsg(sqe, p.fd, p.u.wmsg, MSG_NOSIGNAL);
		break;
	case net_op_type::poll_read:
		io_uring_prep_poll_add(sqe, p.fd, POLLIN);
		break;
	case net_op_type::poll_write:
		io_uring_prep_poll_add(sqe, p.fd, POLLOUT);
		break;
	// recv_ms / recvmsg_ms re-armed via overflow after each CQE
	case net_op_type::recv_ms:
		io_uring_prep_recv(sqe, p.fd, p.u.recv_buf.ptr, p.u.recv_buf.len, 0);
		break;
	case net_op_type::recvmsg_ms:
		io_uring_prep_recvmsg(sqe, p.fd, p.u.rmsg, 0);
		break;
	default:
		io_uring_prep_nop(sqe);
		io_uring_sqe_set_data(sqe, nullptr);
		delete p.ctx;
		m_overflow.pop_front();
		return;
	}
	io_uring_sqe_set_data(sqe, p.ctx);
	m_overflow.pop_front();
}

void io_uring_event_loop::submit()
{
	if (!m_initialized) return;
	std::lock_guard<std::mutex> l(m_ring_mutex);
	io_uring_submit(&m_ring);
}

// ============================================================
// Reaper thread
// ============================================================

void io_uring_event_loop::reaper_thread_fn()
{
	while (true)
	{
		struct io_uring_cqe* cqe = nullptr;
		int ret;

		if (m_abort.load(std::memory_order_acquire))
		{
			// Drain the ring non-blockingly to free all net_completion objects.
			ret = io_uring_peek_cqe(&m_ring, &cqe);
			if (ret != 0 || !cqe) break;
		}
		else
		{
			ret = io_uring_wait_cqe(&m_ring, &cqe);
			if (ret < 0)
			{
				if (ret == -EINTR) continue;
				break;
			}
		}
		if (!cqe) continue;

		auto* ctx       = static_cast<net_completion*>(io_uring_cqe_get_data(cqe));
		int const res   = cqe->res;
		unsigned const flags = cqe->flags;
		io_uring_cqe_seen(&m_ring, cqe);

		// Each consumed CQE frees one SQE slot: flush overflow entries first.
		if (!m_abort.load(std::memory_order_acquire) && !m_overflow.empty())
		{
			std::lock_guard<std::mutex> lk(m_ring_mutex);
			bool flushed = false;
			while (!m_overflow.empty())
			{
				struct io_uring_sqe* s = io_uring_get_sqe(&m_ring);
				if (!s) break;
				flush_one_overflow(s);
				flushed = true;
			}
			if (flushed) io_uring_submit(&m_ring);
		}

		if (!ctx)
		{
			// NOP for shutdown wake-up or cancel CQE — no handler to call.
			continue;
		}

		if (m_abort.load(std::memory_order_acquire))
		{
			// During shutdown: free multishot state only when F_MORE is clear.
			// recv_ms / recvmsg_ms never set F_MORE (they use plain recv),
			// so they are always freed here.
			bool const more = (flags & IORING_CQE_F_MORE) != 0;
			if (!more) delete ctx;
			continue;
		}

		dispatch_cqe(ctx, res, flags);
	}

	// Drain any remaining overflow entries without calling handlers.
	std::deque<pending_submission> leftover;
	{
		std::lock_guard<std::mutex> lk(m_ring_mutex);
		leftover = std::move(m_overflow);
	}
	for (auto& p : leftover)
		delete p.ctx;
}

} // namespace aux
} // namespace libtorrent

#endif // TORRENT_HAVE_IO_URING
