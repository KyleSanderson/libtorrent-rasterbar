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

#ifndef TORRENT_IO_URING_DISK_IO_HPP
#define TORRENT_IO_URING_DISK_IO_HPP

#include "libtorrent/config.hpp"

#if TORRENT_HAVE_IO_URING

#include "libtorrent/disk_interface.hpp"
#include "libtorrent/io_context.hpp"

namespace libtorrent {

	struct counters;
	struct settings_interface;

	// constructs an io_uring-based disk I/O object. Linux only.
	// Uses the Linux io_uring subsystem for truly asynchronous file I/O,
	// replacing mmap-based storage with direct read/write operations
	// submitted through io_uring submission queues.
	TORRENT_EXPORT std::unique_ptr<disk_interface> io_uring_disk_io_constructor(
		io_context& ios, settings_interface const&, counters& cnt);

} // namespace libtorrent

#endif // TORRENT_HAVE_IO_URING

#endif // TORRENT_IO_URING_DISK_IO_HPP
