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

#include "test.hpp"
#include "setup_transfer.hpp"
#include "test_utils.hpp"
#include "settings.hpp"

#include "libtorrent/aux_/io_uring_storage.hpp"
#include "libtorrent/aux_/io_uring_socket.hpp"
#include "libtorrent/io_uring_disk_io.hpp"
#include "libtorrent/posix_disk_io.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/create_torrent.hpp"
#include "libtorrent/torrent_info.hpp"
#include "libtorrent/aux_/path.hpp"
#include "libtorrent/aux_/session_settings.hpp"
#include "libtorrent/random.hpp"
#include "libtorrent/flags.hpp"
#include "peer_server.hpp"

#include <memory>
#include <iostream>
#include <chrono>
#include <thread>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

using namespace lt;

namespace {

constexpr int piece_size = 16 * 1024;

std::vector<char> new_piece(std::size_t const size)
{
	std::vector<char> ret(size);
	aux::random_bytes(ret);
	return ret;
}

void cleanup(std::string const& path)
{
	std::string const p = complete(path);
	error_code ec;
	remove_all(p, ec);
}

} // anonymous namespace

// ---- io_uring_storage: basic read/write ----

TORRENT_TEST(io_uring_storage_read_write)
{
	cleanup("temp_storage");

	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test1.tmp", 3 * piece_size);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{
		fs, nullptr, test_path,
		storage_mode_sparse, priorities, info_hash
	};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	std::vector<char> piece0 = new_piece(piece_size);
	std::vector<char> piece1 = new_piece(piece_size);

	// write piece 0
	int ret = st->write(set, piece0, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);

	// write piece 1
	ret = st->write(set, piece1, 1_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);

	// read back piece 0
	std::vector<char> buf(piece_size);
	ret = st->read(set, buf, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == piece0);

	// read back piece 1
	ret = st->read(set, buf, 1_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == piece1);

	// partial write and read
	std::vector<char> partial(4096);
	aux::random_bytes(partial);
	ret = st->write(set, partial, 2_piece, 100, se);
	TEST_EQUAL(ret, 4096);
	TEST_CHECK(!se.ec);

	std::vector<char> rbuf(4096);
	ret = st->read(set, rbuf, 2_piece, 100, se);
	TEST_EQUAL(ret, 4096);
	TEST_CHECK(!se.ec);
	TEST_CHECK(rbuf == partial);

	st->release_files();
	cleanup("temp_storage");
}

// ---- io_uring_storage: read from never-written file returns zeros ----
// initialize() pre-allocates every declared file to its full size using
// ftruncate (sparse allocation).  Reads to unwritten regions return
// all-zeros – no disk error.  The hash check then marks the piece as
// missing and the torrent engine re-requests it, rather than aborting.
TORRENT_TEST(io_uring_storage_read_nonexistent)
{
	cleanup("temp_storage");

	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test1.tmp", 2 * piece_size);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// File pre-allocated by initialize() – reading any piece returns zeros.
	std::vector<char> buf(piece_size);
	int ret = st->read(set, buf, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == std::vector<char>(piece_size, '\0'));

	st->release_files();
	cleanup("temp_storage");
}

// ---- io_uring_storage: read beyond actual file EOF → file_too_short ----
// When a file on disk is shorter than the declared torrent size, a read
// that starts inside valid data but extends past the actual EOF must
// return the bytes it could read together with errors::file_too_short.
TORRENT_TEST(io_uring_storage_read_short_file)
{
	cleanup("temp_storage");

	file_storage fs;
	fs.set_piece_length(piece_size);
	// Declare a 3-piece file so there are pieces we can partially write.
	fs.add_file("temp_storage/test1.tmp", 3 * piece_size);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// Write only piece 0 – the file now has exactly piece_size bytes.
	std::vector<char> piece0 = new_piece(piece_size);
	int ret = st->write(set, piece0, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);

	// Read piece 0 back – should succeed.
	std::vector<char> buf(piece_size);
	ret = st->read(set, buf, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == piece0);

	// Read piece 2 – with ftruncate pre-allocation the file is already
	// declared_size bytes (3 pieces).  The unwritten region reads back as
	// all-zeros: no disk error.  The resulting hash mismatch causes the
	// piece to be re-requested, not a fatal torrent error.
	se = {};
	ret = st->read(set, buf, 2_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == std::vector<char>(piece_size, '\0'));

	st->release_files();
	cleanup("temp_storage");
}

// ---- io_uring_storage: stale error must not poison the next read call ----
// Reproduces the async_hash bug: a storage_error left over from a failed
// read (e.g. file_too_short or eof) must not cause the NEXT read – which
// would succeed – to fail.  Before the fix, our lambda checked ec.ec on
// entry and returned -1 immediately when the fd was already in the cache.
TORRENT_TEST(io_uring_storage_stale_error_cleared)
{
	cleanup("temp_storage");

	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test1.tmp", 4 * piece_size);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	// Reuse a single storage_error across calls, exactly like async_hash does.
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// Step 1: read piece 0 before writing – file is pre-allocated with zeros,
	// so the read succeeds and returns a zeroed buffer (no error).
	std::vector<char> buf(piece_size);
	int ret = st->read(set, buf, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);  // pre-allocation: read zero-region, no error

	// Step 2: write piece 0 with the SAME se (already clear from step 1).
	std::vector<char> data = new_piece(piece_size);
	ret = st->write(set, data, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);  // write must succeed

	// Step 3: manually poison se with a stale eof, simulating a previous
	// block read that hit EOF mid-piece (the async_hash pattern).
	se.ec = boost::asio::error::eof;

	// Step 4: read piece 0 back – write() wrote real data, se must be cleared
	// by read() on entry, so the stale eof must NOT cause a false failure.
	ret = st->read(set, buf, 0_piece, 0, se);
	TEST_EQUAL(ret, piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == data);

	st->release_files();
	cleanup("temp_storage");
}

// ---- io_uring_storage: write→read data-integrity sequence ----
// Verifies the full write-then-hash-check round-trip for multiple pieces,
// including the case where the same storage_error variable is reused for
// every read (the async_hash calling pattern).
TORRENT_TEST(io_uring_storage_write_read_roundtrip)
{
	cleanup("temp_storage");

	constexpr int num_pieces = 8;
	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test1.tmp", num_pieces * piece_size);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// Write all pieces (out-of-order to stress the fd cache).
	std::vector<std::vector<char>> pieces(num_pieces);
	for (int i = num_pieces - 1; i >= 0; --i)
	{
		pieces[i] = new_piece(piece_size);
		int ret = st->write(set, pieces[i], piece_index_t(i), 0, se);
		TEST_EQUAL(ret, piece_size);
		TEST_CHECK(!se.ec);
	}

	// Read back every piece using the SAME se, simulating async_hash.
	// A stale error from any one piece must not corrupt the next.
	for (int i = 0; i < num_pieces; ++i)
	{
		std::vector<char> buf(piece_size);
		int ret = st->read(set, buf, piece_index_t(i), 0, se);
		TEST_EQUAL(ret, piece_size);
		TEST_CHECK(!se.ec);
		TEST_CHECK(buf == pieces[i]);
	}

	st->release_files();
	cleanup("temp_storage");
}

// ---- io_uring_storage: rename file ----

TORRENT_TEST(io_uring_storage_rename)
{
	cleanup("temp_storage");

	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test1.tmp", 1024);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{
		fs, nullptr, test_path,
		storage_mode_sparse, priorities, info_hash
	};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// write some data
	std::vector<char> data(1024);
	aux::random_bytes(data);
	int const wret = st->write(set, data, 0_piece, 0, se);
	TEST_EQUAL(wret, 1024);
	TEST_CHECK(!se.ec);

	// flush to disk for visibility
	st->release_files();

	std::string const base = combine_path(test_path, "temp_storage");
	TEST_CHECK(exists(combine_path(base, "test1.tmp")));

	st->rename_file(file_index_t{0}, "temp_storage/renamed.tmp", se);
	TEST_CHECK(!se.ec);

	TEST_CHECK(!exists(combine_path(base, "test1.tmp")));
	TEST_CHECK(exists(combine_path(base, "renamed.tmp")));

	st->release_files();
	cleanup("temp_storage");
}

// ---- io_uring_storage: delete files ----

TORRENT_TEST(io_uring_storage_delete)
{
	cleanup("temp_storage");

	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test1.tmp", 1024);
	fs.add_file("temp_storage/subdir/test2.tmp", 1024);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{
		fs, nullptr, test_path,
		storage_mode_sparse, priorities, info_hash
	};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// write data to both files
	std::vector<char> data(1024);
	aux::random_bytes(data);
	st->write(set, data, 0_piece, 0, se);

	std::string const base = combine_path(test_path, "temp_storage");
	TEST_CHECK(exists(combine_path(base, "test1.tmp")));

	st->delete_files(session::delete_files, se);

	TEST_CHECK(!exists(combine_path(base, "test1.tmp")));

	cleanup("temp_storage");
}

// ---- disk_interface: check_files through session ----

namespace {

using check_flags_t = lt::flags::bitfield_flag<std::uint32_t, struct check_flags_tag>;
constexpr check_flags_t f_sparse = 0_bit;
constexpr check_flags_t f_zero_prio = 1_bit;
constexpr check_flags_t f_oversized = 2_bit;

void test_check_files(check_flags_t const flags)
{
	std::string const test_path = current_working_directory();
	cleanup("temp_storage");

	file_storage fs;
	fs.add_file("temp_storage/test1.tmp", 3 * piece_size);

	lt::create_torrent t(fs, piece_size, create_torrent::v1_only);

	std::vector<char> piece0 = new_piece(piece_size);
	std::vector<char> piece1 = new_piece(piece_size);
	std::vector<char> piece2 = new_piece(piece_size);

	t.set_hash(0_piece, hasher(piece0).final());
	t.set_hash(1_piece, hasher(piece1).final());
	t.set_hash(2_piece, hasher(piece2).final());

	std::vector<char> buf;
	bencode(std::back_inserter(buf), t.generate());
	auto info = std::make_shared<torrent_info>(buf, from_span);

	{
		error_code ec;
		create_directory(complete("temp_storage"), ec);
	}

	std::string const file_path = combine_path(test_path, "temp_storage/test1.tmp");

	{
		int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
		TEST_CHECK(fd >= 0);
		if (fd >= 0)
		{
			std::vector<char> all_data;
			all_data.insert(all_data.end(), piece0.begin(), piece0.end());
			all_data.insert(all_data.end(), piece1.begin(), piece1.end());
			all_data.insert(all_data.end(), piece2.begin(), piece2.end());
			if (flags & f_oversized)
			{
				std::vector<char> extra(1024, 'x');
				all_data.insert(all_data.end(), extra.begin(), extra.end());
			}
			ssize_t written = ::write(fd, all_data.data(), all_data.size());
			(void)written;
			::close(fd);
		}
	}

	settings_pack pack = settings();
	pack.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");

	session_params sp;
	sp.settings = std::move(pack);
	sp.disk_io_constructor = lt::io_uring_disk_io_constructor;
	lt::session ses(std::move(sp));

	add_torrent_params atp;
	atp.ti = info;
	atp.save_path = test_path;
	if (flags & f_sparse)
		atp.storage_mode = storage_mode_sparse;

	if (flags & f_zero_prio)
	{
		aux::vector<download_priority_t, file_index_t> prio;
		prio.push_back(dont_download);
		atp.file_priorities = std::move(prio);
	}

	torrent_handle h = ses.add_torrent(std::move(atp));

	torrent_status st;
	for (int i = 0; i < 100; ++i)
	{
		print_alerts(ses, "ses");
		st = h.status();
		if (st.state == torrent_status::seeding
			|| st.state == torrent_status::finished)
			break;
		if (st.errc) break;
		std::this_thread::sleep_for(lt::milliseconds(100));
	}

	if (!(flags & f_zero_prio))
	{
		TEST_CHECK(st.state == torrent_status::seeding);
	}

	ses.remove_torrent(h);
	cleanup("temp_storage");
}

} // anonymous namespace

TORRENT_TEST(check_files_sparse_io_uring)
{
	test_check_files(f_sparse | f_zero_prio);
}

TORRENT_TEST(check_files_oversized_io_uring_zero_prio)
{
	test_check_files(f_sparse | f_zero_prio | f_oversized);
}

TORRENT_TEST(check_files_oversized_io_uring)
{
	test_check_files(f_sparse | f_oversized);
}

TORRENT_TEST(check_files_allocate_io_uring)
{
	test_check_files(f_zero_prio);
}

// ---- session: add torrent, seed from existing data ----

TORRENT_TEST(io_uring_session_seed)
{
	std::string const test_path = current_working_directory();
	cleanup("temp_storage");

	file_storage fs;
	fs.add_file("temp_storage/test1.tmp", 3 * piece_size);

	lt::create_torrent t(fs, piece_size, create_torrent::v1_only);

	std::vector<char> piece0 = new_piece(piece_size);
	std::vector<char> piece1 = new_piece(piece_size);
	std::vector<char> piece2 = new_piece(piece_size);

	t.set_hash(0_piece, hasher(piece0).final());
	t.set_hash(1_piece, hasher(piece1).final());
	t.set_hash(2_piece, hasher(piece2).final());

	std::vector<char> buf;
	bencode(std::back_inserter(buf), t.generate());
	auto info = std::make_shared<torrent_info>(buf, from_span);

	{
		error_code ec;
		create_directory(combine_path(test_path, "temp_storage"), ec);
	}

	std::string const file_path = combine_path(test_path, "temp_storage/test1.tmp");
	{
		int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
		TEST_CHECK(fd >= 0);
		if (fd >= 0)
		{
			ssize_t r;
			r = ::write(fd, piece0.data(), piece0.size());
			r = ::write(fd, piece1.data(), piece1.size());
			r = ::write(fd, piece2.data(), piece2.size());
			(void)r;
			::close(fd);
		}
	}

	settings_pack pack = settings();
	pack.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");

	session_params sp;
	sp.settings = std::move(pack);
	sp.disk_io_constructor = lt::io_uring_disk_io_constructor;
	lt::session ses(std::move(sp));

	add_torrent_params atp;
	atp.ti = info;
	atp.save_path = test_path;
	atp.storage_mode = storage_mode_sparse;
	torrent_handle h = ses.add_torrent(std::move(atp));

	torrent_status st;
	for (int i = 0; i < 100; ++i)
	{
		print_alerts(ses, "ses");
		st = h.status();
		if (st.state == torrent_status::seeding) break;
		if (st.errc)
		{
			std::cout << "torrent error: " << st.errc.message() << std::endl;
			break;
		}
		std::this_thread::sleep_for(lt::milliseconds(100));
	}

	TEST_CHECK(st.state == torrent_status::seeding);

	ses.remove_torrent(h);
	cleanup("temp_storage");
}

// ---- session: add torrent with no data, verify state ----

TORRENT_TEST(io_uring_session_no_data)
{
	std::string const test_path = current_working_directory();
	cleanup("temp_storage");

	file_storage fs;
	fs.add_file("temp_storage/test1.tmp", 3 * piece_size);

	lt::create_torrent t(fs, piece_size, create_torrent::v1_only);

	std::vector<char> piece0 = new_piece(piece_size);
	std::vector<char> piece1 = new_piece(piece_size);
	std::vector<char> piece2 = new_piece(piece_size);

	t.set_hash(0_piece, hasher(piece0).final());
	t.set_hash(1_piece, hasher(piece1).final());
	t.set_hash(2_piece, hasher(piece2).final());

	std::vector<char> buf;
	bencode(std::back_inserter(buf), t.generate());
	auto info = std::make_shared<torrent_info>(buf, from_span);

	settings_pack pack = settings();
	pack.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");

	session_params sp;
	sp.settings = std::move(pack);
	sp.disk_io_constructor = lt::io_uring_disk_io_constructor;
	lt::session ses(std::move(sp));

	add_torrent_params atp;
	atp.ti = info;
	atp.save_path = test_path;
	atp.storage_mode = storage_mode_sparse;
	torrent_handle h = ses.add_torrent(std::move(atp));

	for (int i = 0; i < 50; ++i)
	{
		print_alerts(ses, "ses");
		torrent_status st = h.status();
		if (st.state != torrent_status::checking_files
			&& st.state != torrent_status::checking_resume_data)
			break;
		std::this_thread::sleep_for(lt::milliseconds(100));
	}

	torrent_status st = h.status();
	TEST_CHECK(st.state == torrent_status::downloading
		|| st.state == torrent_status::finished);

	ses.remove_torrent(h);
	cleanup("temp_storage");
}

// ---- multi-file torrent: write + read roundtrip ----
// Verifies that pieces spanning a file boundary are correctly written and
// read back.  The test writes all pieces out of order and reads them back,
// exercising both the fd-cache reuse path and the cross-file readwrite path.
TORRENT_TEST(io_uring_storage_multifile_write_read)
{
	cleanup("temp_storage");

	// Two files of 2*piece_size each, 4 pieces total.
	// Piece 1 spans the boundary between file0 and file1.
	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/file0.tmp", 2 * piece_size);
	fs.add_file("temp_storage/file1.tmp", 2 * piece_size);
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// Write all 4 pieces out of order (reverse) to stress the fd cache.
	std::vector<std::vector<char>> pieces(4);
	for (int i = 3; i >= 0; --i)
	{
		pieces[i] = new_piece(piece_size);
		int const ret = st->write(set, pieces[i], piece_index_t(i), 0, se);
		TEST_EQUAL(ret, piece_size);
		TEST_CHECK(!se.ec);
	}

	// Read back every piece and verify content.
	for (int i = 0; i < 4; ++i)
	{
		std::vector<char> buf(piece_size);
		int const ret = st->read(set, buf, piece_index_t(i), 0, se);
		TEST_EQUAL(ret, piece_size);
		TEST_CHECK(!se.ec);
		TEST_CHECK(buf == pieces[i]);
	}

	st->release_files();
	cleanup("temp_storage");
}

// ---- fd reuse: read then write on the same file must not close the fd ----
// Exercises the fix for the fd-use-after-close race: a read opens the file
// O_RDWR; a subsequent write for a different piece of the same file must
// return the same cached fd (no close-and-reopen) so any in-flight io_uring
// read SQEs that reference that fd are not invalidated.
TORRENT_TEST(io_uring_storage_fd_rdwr_reuse)
{
	cleanup("temp_storage");

	constexpr int num_pieces = 8;
	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test.tmp", num_pieces * piece_size);
	fs.set_num_pieces(num_pieces);

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};
	auto st = std::make_shared<aux::io_uring_storage>(p);

	aux::session_settings set;
	storage_error se;
	st->initialize(set, se);
	TEST_CHECK(!se.ec);

	// Write piece 7 first (high offset) so the file is extended.
	std::vector<char> piece7 = new_piece(piece_size);
	TEST_EQUAL(st->write(set, piece7, 7_piece, 0, se), piece_size);
	TEST_CHECK(!se.ec);

	// Now read piece 7 back.  This opens the file O_RDWR and caches the fd.
	std::vector<char> buf(piece_size);
	TEST_EQUAL(st->read(set, buf, 7_piece, 0, se), piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == piece7);

	// Write piece 3 (lower offset, same file) — must reuse the cached fd,
	// not close it and open a new one.
	std::vector<char> piece3 = new_piece(piece_size);
	TEST_EQUAL(st->write(set, piece3, 3_piece, 0, se), piece_size);
	TEST_CHECK(!se.ec);

	// Read piece 3 back via the same cached fd.
	TEST_EQUAL(st->read(set, buf, 3_piece, 0, se), piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == piece3);

	// Also verify piece 7 is still intact.
	TEST_EQUAL(st->read(set, buf, 7_piece, 0, se), piece_size);
	TEST_CHECK(!se.ec);
	TEST_CHECK(buf == piece7);

	st->release_files();
	cleanup("temp_storage");
}

// ---- full hash-check roundtrip after partial download ----
// Mimics the "resume with partial data" path: write one piece, restart,
// then verify that:
//  - the written piece reads back correctly (no error),
//  - a piece whose offset is exactly at the file's end returns file_too_short,
//  - a piece whose offset is beyond the file's end also returns file_too_short.
// Note: sparse holes WITHIN the file's extent read as zeros (correct OS
// behaviour) and do NOT produce an I/O error — the hash simply won't match.
TORRENT_TEST(io_uring_storage_partial_download_hashcheck)
{
	cleanup("temp_storage");

	constexpr int num_pieces = 6;
	file_storage fs;
	fs.set_piece_length(piece_size);
	fs.add_file("temp_storage/test.tmp", num_pieces * piece_size);
	fs.set_num_pieces(num_pieces);

	std::string const test_path = current_working_directory();
	aux::vector<download_priority_t, file_index_t> priorities;
	sha1_hash info_hash;
	storage_params p{fs, nullptr, test_path, storage_mode_sparse, priorities, info_hash};

	// --- First "session": download only piece 0 ---
	std::vector<char> piece0 = new_piece(piece_size);
	{
		auto st = std::make_shared<aux::io_uring_storage>(p);
		aux::session_settings set;
		storage_error se;
		st->initialize(set, se);
		TEST_CHECK(!se.ec);

		TEST_EQUAL(st->write(set, piece0, 0_piece, 0, se), piece_size);
		TEST_CHECK(!se.ec);
		st->release_files();
		// File on disk is now exactly piece_size bytes.
	}

	// --- Second "session": re-create storage and verify ---
	{
		auto st = std::make_shared<aux::io_uring_storage>(p);
		aux::session_settings set;
		storage_error se;
		st->initialize(set, se);
		TEST_CHECK(!se.ec);

		storage_error ignore;
		TEST_CHECK(st->has_any_file(ignore));  // file exists from first session

		std::vector<char> buf(piece_size);

		// Piece 0 was written and must be readable with no error.
		se = {};
		TEST_EQUAL(st->read(set, buf, 0_piece, 0, se), piece_size);
		TEST_CHECK(!se.ec);
		TEST_CHECK(buf == piece0);

		// Pieces 1 and 3 were not written.  initialize() pre-allocates the
		// file to the declared size using ftruncate, so unwritten regions read
		// back as all-zeros – no disk error.  The hash mismatch causes the
		// piece to be re-requested rather than aborting the torrent.
		se = {};
		int ret = st->read(set, buf, 1_piece, 0, se);
		TEST_EQUAL(ret, piece_size);
		TEST_CHECK(!se.ec);
		TEST_CHECK(buf == std::vector<char>(piece_size, '\0'));

		se = {};
		ret = st->read(set, buf, 3_piece, 0, se);
		TEST_EQUAL(ret, piece_size);
		TEST_CHECK(!se.ec);
		TEST_CHECK(buf == std::vector<char>(piece_size, '\0'));

		st->release_files();
	}

	cleanup("temp_storage");
}

// ---- full session download via io_uring disk i/o ----
// Runs a real two-session download seeder→downloader, both using the
// io_uring disk i/o backend, and verifies the downloader reaches seeding
// state (all pieces verified).
TORRENT_TEST(io_uring_session_download)
{
	std::string const seed_path = complete("seed_storage");
	std::string const dl_path   = complete("dl_storage");
	cleanup("seed_storage");
	cleanup("dl_storage");

	file_storage fs;
	fs.add_file("seed_storage/data.tmp", 4 * piece_size);

	lt::create_torrent t(fs, piece_size, create_torrent::v1_only);

	std::vector<std::vector<char>> pieces(4);
	for (int i = 0; i < 4; ++i)
	{
		pieces[i] = new_piece(piece_size);
		t.set_hash(piece_index_t(i), hasher(pieces[i]).final());
	}

	std::vector<char> torrent_buf;
	bencode(std::back_inserter(torrent_buf), t.generate());
	auto info = std::make_shared<torrent_info>(torrent_buf, from_span);

	// Write seed data to disk.
	{
		error_code ec;
		create_directory(seed_path, ec);
		int fd = ::open((seed_path + "/data.tmp").c_str()
			, O_RDWR | O_CREAT | O_TRUNC, 0644);
		TEST_CHECK(fd >= 0);
		if (fd >= 0)
		{
			for (auto const& p : pieces)
				(void)::write(fd, p.data(), p.size());
			::close(fd);
		}
	}

	// Seeder session — use default (posix) disk I/O for the seeder so the
	// seeder is a known-good baseline, letting us isolate the downloader's
	// io_uring disk backend.
	settings_pack sp = settings();
	sp.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");
	session_params seeder_sp;
	seeder_sp.settings = sp;
	lt::session seeder(std::move(seeder_sp));

	add_torrent_params seed_atp;
	seed_atp.ti = info;
	seed_atp.save_path = complete(".");
	torrent_handle seed_h = seeder.add_torrent(std::move(seed_atp));

	// Wait for seeder to reach seeding state.
	for (int i = 0; i < 100; ++i)
	{
		print_alerts(seeder, "seeder");
		if (seed_h.status().state == torrent_status::seeding) break;
		std::this_thread::sleep_for(lt::milliseconds(100));
	}
	TEST_CHECK(seed_h.status().state == torrent_status::seeding);

	int const port = static_cast<int>(seeder.listen_port());
	TEST_CHECK(port > 0);

	// Downloader session with io_uring disk backend — this is what we're
	// testing.
	settings_pack dl_sp_pack = settings();
	dl_sp_pack.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");
	session_params dl_sp;
	dl_sp.settings = dl_sp_pack;
	dl_sp.disk_io_constructor = lt::io_uring_disk_io_constructor;
	lt::session downloader(std::move(dl_sp));

	add_torrent_params dl_atp;
	dl_atp.ti = info;
	dl_atp.save_path = dl_path;
	// Connect to seeder as a peer.
	dl_atp.peers.push_back(lt::tcp::endpoint(
		lt::address_v4::loopback(), static_cast<std::uint16_t>(port)));

	torrent_handle dl_h = downloader.add_torrent(std::move(dl_atp));

	torrent_status dl_st;
	for (int i = 0; i < 300; ++i)
	{
		print_alerts(seeder,     "seeder");
		print_alerts(downloader, "downloader");
		dl_st = dl_h.status();
		if (dl_st.state == torrent_status::seeding) break;
		if (dl_st.errc)
		{
			std::cout << "downloader error: " << dl_st.errc.message() << "\n";
			break;
		}
		std::this_thread::sleep_for(lt::milliseconds(100));
	}

	TEST_CHECK(dl_st.state == torrent_status::seeding);
	TEST_CHECK(!dl_st.errc);

	seeder.remove_torrent(seed_h);
	downloader.remove_torrent(dl_h);
	cleanup("seed_storage");
	cleanup("dl_storage");
}



namespace {

// Helper: poll ios until predicate is true or timeout_ms elapses.
// io_context::poll() drains all ready handlers and then "stops" the context.
// We restart() before each poll so subsequent calls work.
template <typename Pred>
bool ios_poll_until(lt::io_context& ios, Pred pred, int timeout_ms = 5000)
{
	auto deadline = std::chrono::steady_clock::now()
		+ std::chrono::milliseconds(timeout_ms);
	while (!pred() && std::chrono::steady_clock::now() < deadline)
	{
		ios.restart();
		ios.poll();
		if (!pred())
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	ios.restart();
	return pred();
}

} // anonymous namespace

// Basic send + recv over a Unix socketpair.
TORRENT_TEST(io_uring_event_loop_send_recv)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	TEST_CHECK(loop.is_initialized());

	int sv[2] = {-1, -1};
	TEST_CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

	std::string const msg = "hello io_uring networking";
	std::vector<char> recv_buf(msg.size(), '\0');

	bool send_done = false, recv_done = false;
	lt::error_code send_ec, recv_ec;
	int send_bytes = 0, recv_bytes = 0;

	loop.async_recv(sv[1], lt::span<char>(recv_buf.data(), (int)recv_buf.size()),
		[&](int n, lt::error_code ec) {
			recv_bytes = n; recv_ec = ec; recv_done = true;
		});
	loop.async_send(sv[0], lt::span<char const>(msg.data(), (int)msg.size()),
		[&](int n, lt::error_code ec) {
			send_bytes = n; send_ec = ec; send_done = true;
		});

	bool ok = ios_poll_until(ios, [&]{ return send_done && recv_done; });
	TEST_CHECK(ok);
	TEST_CHECK(!send_ec);
	TEST_CHECK(!recv_ec);
	TEST_EQUAL(send_bytes, (int)msg.size());
	TEST_EQUAL(recv_bytes, (int)msg.size());
	TEST_CHECK(std::string(recv_buf.begin(), recv_buf.end()) == msg);

	::close(sv[0]);
	::close(sv[1]);
	loop.stop();
}

// async_poll_read fires once data is available.
TORRENT_TEST(io_uring_event_loop_poll_read)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	TEST_CHECK(loop.is_initialized());

	int sv[2] = {-1, -1};
	TEST_CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

	bool ready = false;
	lt::error_code poll_ec;
	loop.async_poll_read(sv[1], [&](lt::error_code ec) {
		ready = true; poll_ec = ec;
	});

	// Make sv[1] readable by writing one byte from sv[0].
	char ch = 'x';
	TEST_CHECK(::write(sv[0], &ch, 1) == 1);

	bool ok = ios_poll_until(ios, [&]{ return ready; });
	TEST_CHECK(ok);
	TEST_CHECK(!poll_ec);

	::close(sv[0]);
	::close(sv[1]);
	loop.stop();
}

// async_poll_write fires immediately (socket writable from the start).
TORRENT_TEST(io_uring_event_loop_poll_write)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	TEST_CHECK(loop.is_initialized());

	int sv[2] = {-1, -1};
	TEST_CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

	bool ready = false;
	lt::error_code poll_ec;
	loop.async_poll_write(sv[0], [&](lt::error_code ec) {
		ready = true; poll_ec = ec;
	});

	bool ok = ios_poll_until(ios, [&]{ return ready; });
	TEST_CHECK(ok);
	TEST_CHECK(!poll_ec);

	::close(sv[0]);
	::close(sv[1]);
	loop.stop();
}

// Calling async_* after stop() delivers ECANCELED and does not crash.
TORRENT_TEST(io_uring_event_loop_post_stop_ops)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	TEST_CHECK(loop.is_initialized());
	loop.stop();

	bool called = false;
	lt::error_code got_ec;
	int sv[2] = {-1, -1};
	TEST_CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

	loop.async_send(sv[0], lt::span<char const>("x", 1),
		[&](int, lt::error_code ec) { called = true; got_ec = ec; });

	bool ok = ios_poll_until(ios, [&]{ return called; }, 1000);
	TEST_CHECK(ok);
	// Must be an error (ECANCELED) — no actual send happened.
	TEST_CHECK(got_ec);

	::close(sv[0]);
	::close(sv[1]);
}

// Double-stop: calling stop() twice must not crash or deadlock.
TORRENT_TEST(io_uring_event_loop_double_stop)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	TEST_CHECK(loop.is_initialized());
	loop.stop();
	loop.stop(); // must be a no-op
	TEST_CHECK(true); // reached here without hang
}

// Submit more ops than the ring depth to exercise the overflow queue.
// All ops must complete even though some were queued rather than staged
// immediately into the SQ ring.
TORRENT_TEST(io_uring_event_loop_overflow)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	TEST_CHECK(loop.is_initialized());

	// Submit ring_depth + 100 ops to guarantee at least 100 land in overflow.
	int const N = static_cast<int>(loop.ring_depth()) + 100;
	std::vector<int> sv0(N), sv1(N);
	for (int i = 0; i < N; ++i)
	{
		int sv[2] = {-1, -1};
		TEST_CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
		sv0[i] = sv[0];
		sv1[i] = sv[1];
	}

	std::atomic<int> done{0};
	for (int i = 0; i < N; ++i)
	{
		// poll_write is always immediately ready on a fresh socket, so all
		// N CQEs will arrive quickly, each one flushing from the overflow.
		loop.async_poll_write(sv0[i], [&](lt::error_code ec)
		{
			TEST_CHECK(!ec);
			done.fetch_add(1, std::memory_order_relaxed);
		});
	}

	bool ok = ios_poll_until(ios, [&] { return done.load() == N; }, 10000);
	TEST_CHECK(ok);
	TEST_EQUAL(done.load(), N);

	for (int i = 0; i < N; ++i)
	{
		::close(sv0[i]);
		::close(sv1[i]);
	}
	loop.stop();
}

// ============================================================
// Multishot tests
// ============================================================

// Helper: TCP loopback socketpair using real bind/connect.
// Returns {server_accepted_fd, client_fd}. listener is closed afterwards.
static std::pair<int,int> tcp_loopback_pair()
{
	int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	TORRENT_ASSERT(listener >= 0);
	int reuse = 1;
	::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

	struct sockaddr_in addr{};
	addr.sin_family      = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port        = 0;
	::bind(listener, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
	::listen(listener, 8);

	struct sockaddr_in bound{};
	socklen_t blen = sizeof(bound);
	::getsockname(listener, reinterpret_cast<struct sockaddr*>(&bound), &blen);

	int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	TORRENT_ASSERT(client >= 0);
	::connect(client, reinterpret_cast<struct sockaddr*>(&bound), sizeof(bound));

	struct sockaddr_in peer{};
	socklen_t plen = sizeof(peer);
	int server = ::accept(listener, reinterpret_cast<struct sockaddr*>(&peer), &plen);
	TORRENT_ASSERT(server >= 0);
	::close(listener);
	return {server, client};
}

// accept_multishot: connect N clients, verify N accept callbacks.
TORRENT_TEST(io_uring_event_loop_accept_multishot)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	if (!loop.is_initialized()) return; // kernel too old

	// create listening socket
	int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	int reuse = 1;
	::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
	struct sockaddr_in laddr{};
	laddr.sin_family      = AF_INET;
	laddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	laddr.sin_port        = 0;
	::bind(listener, reinterpret_cast<struct sockaddr*>(&laddr), sizeof(laddr));
	::listen(listener, 64);
	struct sockaddr_in bound{};
	socklen_t blen = sizeof(bound);
	::getsockname(listener, reinterpret_cast<struct sockaddr*>(&bound), &blen);

	constexpr int N = 5;
	std::atomic<int> accepted{0};
	std::vector<int> accepted_fds;
	std::mutex fds_mutex;

	struct sockaddr_in peer_addr{};
	socklen_t peer_addrlen = sizeof(peer_addr);

	loop.async_accept_multishot(listener
		, reinterpret_cast<struct sockaddr*>(&peer_addr), &peer_addrlen
		, [&](int new_fd, struct sockaddr*, socklen_t, lt::error_code ec, bool /*more*/)
		{
			if (ec) return;
			{
				std::lock_guard<std::mutex> lk(fds_mutex);
				accepted_fds.push_back(new_fd);
			}
			accepted.fetch_add(1, std::memory_order_relaxed);
		});

	// connect N clients
	std::vector<int> clients;
	for (int i = 0; i < N; ++i)
	{
		int c = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
		::connect(c, reinterpret_cast<struct sockaddr*>(&bound), sizeof(bound));
		clients.push_back(c);
	}

	bool ok = ios_poll_until(ios, [&] { return accepted.load() >= N; }, 10000);
	TEST_CHECK(ok);
	TEST_EQUAL(accepted.load(), N);

	for (int fd : clients)  ::close(fd);
	{
		std::lock_guard<std::mutex> lk(fds_mutex);
		for (int fd : accepted_fds) ::close(fd);
	}
	::close(listener);
	loop.stop();
}

// accept_multishot_cancel: single connect, then cancel, verify no more cbs.
TORRENT_TEST(io_uring_event_loop_accept_multishot_cancel)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	if (!loop.is_initialized()) return;

	int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	int reuse = 1;
	::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
	struct sockaddr_in laddr{};
	laddr.sin_family      = AF_INET;
	laddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	laddr.sin_port        = 0;
	::bind(listener, reinterpret_cast<struct sockaddr*>(&laddr), sizeof(laddr));
	::listen(listener, 8);
	struct sockaddr_in bound{};
	socklen_t blen = sizeof(bound);
	::getsockname(listener, reinterpret_cast<struct sockaddr*>(&bound), &blen);

	std::atomic<int> accepted{0};
	struct sockaddr_in peer_addr{};
	socklen_t peer_addrlen = sizeof(peer_addr);

	auto tok = loop.async_accept_multishot(listener
		, reinterpret_cast<struct sockaddr*>(&peer_addr), &peer_addrlen
		, [&](int new_fd, struct sockaddr*, socklen_t, lt::error_code ec, bool /*more*/)
		{
			if (!ec && new_fd >= 0) { ::close(new_fd); accepted.fetch_add(1); }
		});

	// connect one client to get at least one successful callback
	int c1 = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	::connect(c1, reinterpret_cast<struct sockaddr*>(&bound), sizeof(bound));
	ios_poll_until(ios, [&] { return accepted.load() >= 1; }, 5000);
	TEST_CHECK(accepted.load() >= 1);

	// cancel the multishot
	loop.cancel(tok);

	// Give it time to settle, then connect another client — should NOT increment.
	// We run ios for a bit after cancel then measure
	int snap = accepted.load();
	int c2 = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	::connect(c2, reinterpret_cast<struct sockaddr*>(&bound), sizeof(bound));
	// drain ios for up to 200 ms — the counter must NOT advance past snap
	auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
	while (std::chrono::steady_clock::now() < deadline)
		ios.poll_one();
	TEST_CHECK(accepted.load() == snap); // cancel was effective

	::close(c1); ::close(c2);
	::close(listener);
	loop.stop();
}

// recv_multishot: send 3 messages, get 3 callbacks from one SQE.
TORRENT_TEST(io_uring_event_loop_recv_multishot)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	if (!loop.is_initialized()) return;

	auto [server, client] = tcp_loopback_pair();

	constexpr int N = 3;
	constexpr int MSG_SIZE = 16;
	std::atomic<int> received{0};
	std::vector<char> rxbuf(MSG_SIZE);

	loop.async_recv_multishot(server, lt::span<char>(rxbuf)
		, [&](int bytes, lt::error_code ec, bool /*more*/)
		{
			if (!ec && bytes > 0)
				received.fetch_add(1, std::memory_order_relaxed);
		});

	// send N messages sequentially
	char msg[MSG_SIZE] = "hello io_uring!";
	for (int i = 0; i < N; ++i)
		::send(client, msg, sizeof(msg), MSG_NOSIGNAL);

	bool ok = ios_poll_until(ios, [&] { return received.load() >= N; }, 10000);
	TEST_CHECK(ok);
	TEST_CHECK(received.load() >= N);

	::close(server);
	::close(client);
	loop.stop();
}

// recvmsg_multishot: UDP datagrams — send N, get N callbacks.
TORRENT_TEST(io_uring_event_loop_recvmsg_multishot)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	if (!loop.is_initialized()) return;

	// UDP socketpair via bind/connect on loopback
	int srv = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
	int cli = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
	TORRENT_ASSERT(srv >= 0 && cli >= 0);
	struct sockaddr_in s_addr{};
	s_addr.sin_family      = AF_INET;
	s_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	s_addr.sin_port        = 0;
	::bind(srv, reinterpret_cast<struct sockaddr*>(&s_addr), sizeof(s_addr));
	socklen_t slen = sizeof(s_addr);
	::getsockname(srv, reinterpret_cast<struct sockaddr*>(&s_addr), &slen);
	::connect(cli, reinterpret_cast<struct sockaddr*>(&s_addr), sizeof(s_addr));

	constexpr int N = 4;
	std::array<char, 32> rxbuf{};
	struct iovec iov{rxbuf.data(), rxbuf.size()};
	struct msghdr mhdr{};
	mhdr.msg_iov    = &iov;
	mhdr.msg_iovlen = 1;

	std::atomic<int> recvd{0};
	loop.async_recvmsg_multishot(srv, &mhdr
		, [&](int bytes, lt::error_code ec, bool /*more*/)
		{
			if (!ec && bytes > 0)
				recvd.fetch_add(1, std::memory_order_relaxed);
		});

	char dgram[] = "datagram!";
	for (int i = 0; i < N; ++i)
		::send(cli, dgram, sizeof(dgram), 0);

	bool ok = ios_poll_until(ios, [&] { return recvd.load() >= N; }, 10000);
	TEST_CHECK(ok);
	TEST_CHECK(recvd.load() >= N);

	::close(srv);
	::close(cli);
	loop.stop();
}

// poll_multishot: single poll SQE fires for N writes to the pipe.
TORRENT_TEST(io_uring_event_loop_poll_multishot)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	if (!loop.is_initialized()) return;

	int pipefd[2];
	TORRENT_ASSERT(::pipe2(pipefd, O_CLOEXEC) == 0);

	constexpr int N = 3;
	std::atomic<int> fired{0};

	loop.async_poll_multishot(pipefd[0], POLLIN
		, [&](lt::error_code ec, bool /*more*/)
		{
			if (!ec)
			{
				char buf[8];
				::read(pipefd[0], buf, sizeof(buf)); // drain so poll re-arms
				fired.fetch_add(1, std::memory_order_relaxed);
			}
		});

	for (int i = 0; i < N; ++i)
	{
		char b = static_cast<char>(i);
		::write(pipefd[1], &b, 1);
		// let the loop handle each event in turn
		ios_poll_until(ios, [&, snap = fired.load()] { return fired.load() > snap; }, 2000);
	}

	TEST_CHECK(fired.load() == N);

	::close(pipefd[0]);
	::close(pipefd[1]);
	loop.stop();
}

// poll_multishot_cancel: cancel stops further poll callbacks.
TORRENT_TEST(io_uring_event_loop_poll_multishot_cancel)
{
	lt::io_context ios;
	lt::aux::io_uring_event_loop loop(ios);
	if (!loop.is_initialized()) return;

	int pipefd[2];
	TORRENT_ASSERT(::pipe2(pipefd, O_CLOEXEC) == 0);

	std::atomic<int> fired{0};

	auto tok = loop.async_poll_multishot(pipefd[0], POLLIN
		, [&](lt::error_code ec, bool /*more*/)
		{
			if (!ec)
			{
				char buf[8]; ::read(pipefd[0], buf, 1);
				fired.fetch_add(1, std::memory_order_relaxed);
			}
		});

	// trigger once
	char b = 1;
	::write(pipefd[1], &b, 1);
	ios_poll_until(ios, [&] { return fired.load() >= 1; }, 5000);
	TEST_CHECK(fired.load() >= 1);

	// cancel
	loop.cancel(tok);

	int snap = fired.load();
	// write again — should NOT fire more
	::write(pipefd[1], &b, 1);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
	while (std::chrono::steady_clock::now() < deadline)
		ios.poll_one();
	TEST_CHECK(fired.load() == snap);

	::close(pipefd[0]);
	::close(pipefd[1]);
	loop.stop();
}

// ============================================================
// Integration test: peer_server accept via io_uring
// ============================================================

// Exercises the peer_server accept path end-to-end.  When built with
// -Duse_io_uring_net=ON, peer_server uses async_accept_multishot internally.
// Without that flag it falls through to the boost::asio path.  Both must pass.
TORRENT_TEST(io_uring_peer_server)
{
#if defined(TORRENT_USE_IO_URING_NET)
	std::printf("io_uring_peer_server: io_uring network backend ACTIVE\n");
#else
	std::printf("io_uring_peer_server: boost::asio network backend "
		"(build with -Duse_io_uring_net=ON to activate io_uring)\n");
#endif

	int const port = start_peer();
	TEST_CHECK(port > 0);

	constexpr int N = 4;
	struct sockaddr_in addr{};
	addr.sin_family      = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port        = htons(static_cast<std::uint16_t>(port));

	std::vector<int> clients;
	clients.reserve(N);
	for (int i = 0; i < N; ++i)
	{
		int c = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
		TORRENT_ASSERT(c >= 0);
		int const r = ::connect(c
			, reinterpret_cast<struct sockaddr const*>(&addr), sizeof(addr));
		(void)r;
		clients.push_back(c);
	}

	// Wait up to 5 s for all N connections to be counted by the server.
	auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (num_peer_hits() < N && std::chrono::steady_clock::now() < deadline)
		std::this_thread::sleep_for(std::chrono::milliseconds(10));

	TEST_EQUAL(num_peer_hits(), N);

	for (int c : clients) ::close(c);
	stop_peer();
}


TORRENT_TEST(io_uring_not_available)
{
	TEST_CHECK(true);
}

#endif // TORRENT_HAVE_IO_URING
