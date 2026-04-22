"""
Integration tests for Memcached meta protocol support (memcache_meta).

These tests start a real memcached instance and run memtier_benchmark
against it using --protocol=memcache_meta.
"""

import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import unittest

MEMTIER_BINARY = os.environ.get("MEMTIER_BINARY",
                                os.path.join(os.path.dirname(__file__), "..", "memtier_benchmark"))
MEMCACHED_BINARY = os.environ.get("MEMCACHED_BINARY", "memcached")


def find_free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def wait_for_port(port, timeout=5):
    """Wait until a TCP port is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=1):
                return True
        except OSError:
            time.sleep(0.1)
    return False


class MemcachedMetaProtocolTests(unittest.TestCase):
    """Test suite for memcache_meta protocol."""

    memcached_proc = None
    memcached_port = None

    @classmethod
    def setUpClass(cls):
        """Start a memcached instance for the test suite."""
        cls.memcached_port = find_free_port()
        cls.memcached_proc = subprocess.Popen(
            [MEMCACHED_BINARY, '-p', str(cls.memcached_port), '-l', '127.0.0.1'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not wait_for_port(cls.memcached_port):
            cls.memcached_proc.kill()
            raise RuntimeError(f"memcached failed to start on port {cls.memcached_port}")

    @classmethod
    def tearDownClass(cls):
        """Stop memcached."""
        if cls.memcached_proc:
            cls.memcached_proc.send_signal(signal.SIGTERM)
            cls.memcached_proc.wait(timeout=5)

    def _run_memtier(self, extra_args=None, expect_success=True):
        """Run memtier_benchmark with memcache_meta protocol and return (ok, results_dir)."""
        test_dir = tempfile.mkdtemp()
        stdout_file = os.path.join(test_dir, 'mb.stdout')
        json_file = os.path.join(test_dir, 'mb.json')

        args = [
            MEMTIER_BINARY,
            '--server', '127.0.0.1',
            '--port', str(self.memcached_port),
            '--protocol', 'memcache_meta',
            '--out-file', stdout_file,
            '--json-out-file', json_file,
            '--hide-histogram',
        ]
        if extra_args:
            args.extend(extra_args)

        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _stdout, _stderr = proc.communicate(timeout=120)

        # Save stderr for debugging
        stderr_path = os.path.join(test_dir, 'mb.stderr')
        with open(stderr_path, 'wb') as f:
            f.write(_stderr)

        ok = proc.returncode == 0
        if expect_success and not ok:
            print(f"memtier_benchmark failed (exit {proc.returncode})")
            print(f"stderr: {_stderr.decode(errors='replace')}")
        return ok, test_dir

    def _load_json(self, results_dir):
        """Load the JSON results file."""
        json_file = os.path.join(results_dir, 'mb.json')
        with open(json_file) as f:
            return json.load(f)

    # ------------------------------------------------------------------
    # Test: basic set-only workload
    # ------------------------------------------------------------------
    def test_set_only(self):
        """Verify set-only workload (ratio 1:0) completes successfully."""
        ok, results_dir = self._run_memtier([
            '--threads', '2',
            '--clients', '5',
            '--requests', '1000',
            '--ratio', '1:0',
            '--pipeline', '10',
        ])
        self.assertTrue(ok, "memtier_benchmark should exit 0 for set-only workload")

        results = self._load_json(results_dir)
        sets = results['ALL STATS']['Sets']
        self.assertGreater(sets['Count'], 0, "Should have performed sets")
        self.assertGreater(sets['Ops/sec'], 0, "Ops/sec should be > 0")
        self.assertGreater(sets['Latency'], 0, "Latency should be > 0")

    # ------------------------------------------------------------------
    # Test: set then get (preload + mixed workload)
    # ------------------------------------------------------------------
    def test_set_then_get(self):
        """Preload keys with sets, then run a mixed set/get workload."""
        # Phase 1: preload 1000 keys
        ok, _ = self._run_memtier([
            '--threads', '1',
            '--clients', '1',
            '--requests', '1000',
            '--ratio', '1:0',
            '--key-pattern', 'P:P',
            '--key-minimum', '1',
            '--key-maximum', '1000',
            '--pipeline', '10',
        ])
        self.assertTrue(ok, "Preload phase should succeed")

        # Phase 2: mixed 1:1 workload reading back keys
        ok, results_dir = self._run_memtier([
            '--threads', '2',
            '--clients', '5',
            '--requests', '500',
            '--ratio', '1:1',
            '--key-pattern', 'R:R',
            '--key-minimum', '1',
            '--key-maximum', '1000',
            '--pipeline', '10',
        ])
        self.assertTrue(ok, "Mixed set/get phase should succeed")

        results = self._load_json(results_dir)
        sets = results['ALL STATS']['Sets']
        gets = results['ALL STATS']['Gets']
        self.assertGreater(sets['Count'], 0, "Should have sets")
        self.assertGreater(gets['Count'], 0, "Should have gets")
        self.assertGreater(gets['Ops/sec'], 0, "Get ops/sec should be > 0")

    # ------------------------------------------------------------------
    # Test: get-only workload (verifies EN/miss handling)
    # ------------------------------------------------------------------
    def test_get_only_cold(self):
        """Get-only against empty keyspace — all misses, should not error."""
        # Flush memcached first via meta protocol: use text flush_all
        with socket.create_connection(('127.0.0.1', self.memcached_port)) as s:
            s.sendall(b'flush_all\r\n')
            s.recv(1024)

        ok, results_dir = self._run_memtier([
            '--threads', '1',
            '--clients', '1',
            '--requests', '100',
            '--ratio', '0:1',
            '--pipeline', '1',
        ])
        self.assertTrue(ok, "Get-only workload on cold cache should succeed")

        results = self._load_json(results_dir)
        gets = results['ALL STATS']['Gets']
        self.assertGreater(gets['Count'], 0, "Should have attempted gets")

    # ------------------------------------------------------------------
    # Test: pipelined workload
    # ------------------------------------------------------------------
    def test_pipelined(self):
        """Verify pipelining works correctly with meta protocol."""
        ok, results_dir = self._run_memtier([
            '--threads', '2',
            '--clients', '5',
            '--requests', '2000',
            '--ratio', '1:1',
            '--pipeline', '50',
        ])
        self.assertTrue(ok, "Pipelined workload should succeed")

        results = self._load_json(results_dir)
        totals = results['ALL STATS']['Totals']
        self.assertGreater(totals['Count'], 0)
        self.assertGreater(totals['Ops/sec'], 0)

    # ------------------------------------------------------------------
    # Test: expiry flag (TTL via T flag in ms command)
    # ------------------------------------------------------------------
    def test_set_with_expiry(self):
        """Verify that sets with expiry range work."""
        ok, results_dir = self._run_memtier([
            '--threads', '1',
            '--clients', '1',
            '--requests', '100',
            '--ratio', '1:0',
            '--expiry-range', '10-20',
            '--pipeline', '1',
        ])
        self.assertTrue(ok, "Set with expiry should succeed")

        results = self._load_json(results_dir)
        sets = results['ALL STATS']['Sets']
        self.assertGreater(sets['Count'], 0)

    # ------------------------------------------------------------------
    # Test: JSON output structure is complete
    # ------------------------------------------------------------------
    def test_json_output_structure(self):
        """Verify JSON output has expected sections and metrics."""
        ok, results_dir = self._run_memtier([
            '--threads', '2',
            '--clients', '5',
            '--requests', '500',
            '--ratio', '1:1',
        ])
        self.assertTrue(ok)

        results = self._load_json(results_dir)

        # Top-level sections
        self.assertIn('ALL STATS', results)
        all_stats = results['ALL STATS']
        self.assertIn('Sets', all_stats)
        self.assertIn('Gets', all_stats)
        self.assertIn('Totals', all_stats)

        # Per-command metrics
        for cmd in ['Sets', 'Gets', 'Totals']:
            metrics = all_stats[cmd]
            self.assertIn('Count', metrics)
            self.assertIn('Ops/sec', metrics)
            self.assertIn('Latency', metrics)
            self.assertIn('KB/sec', metrics)

    # ------------------------------------------------------------------
    # Test: multi-key get (--multi-key-get)
    # ------------------------------------------------------------------
    def test_multi_key_get(self):
        """Verify multi-key-get pipelining works with meta protocol."""
        # Preload some keys
        ok, _ = self._run_memtier([
            '--threads', '1',
            '--clients', '1',
            '--requests', '500',
            '--ratio', '1:0',
            '--key-pattern', 'P:P',
            '--key-minimum', '1',
            '--key-maximum', '500',
        ])
        self.assertTrue(ok, "Preload should succeed")

        # Multi-key get
        ok, results_dir = self._run_memtier([
            '--threads', '1',
            '--clients', '1',
            '--requests', '100',
            '--ratio', '0:1',
            '--multi-key-get', '10',
            '--key-minimum', '1',
            '--key-maximum', '500',
        ])
        self.assertTrue(ok, "Multi-key-get should succeed with meta protocol")

    # ------------------------------------------------------------------
    # Test: various data sizes
    # ------------------------------------------------------------------
    def test_various_data_sizes(self):
        """Verify the protocol handles different value sizes correctly."""
        for data_size in [1, 64, 1024, 4096]:
            ok, results_dir = self._run_memtier([
                '--threads', '1',
                '--clients', '1',
                '--requests', '50',
                '--ratio', '1:0',
                '--data-size', str(data_size),
                '--pipeline', '1',
            ])
            self.assertTrue(ok, f"Set with data-size={data_size} should succeed")

            results = self._load_json(results_dir)
            self.assertEqual(results['ALL STATS']['Sets']['Count'], 50,
                             f"Should complete all 50 sets at data-size={data_size}")

    # ------------------------------------------------------------------
    # Test: single request (minimal smoke test)
    # ------------------------------------------------------------------
    def test_single_request(self):
        """Minimal smoke test: 1 thread, 1 client, 1 request."""
        ok, results_dir = self._run_memtier([
            '--threads', '1',
            '--clients', '1',
            '--requests', '1',
            '--ratio', '1:0',
        ])
        self.assertTrue(ok, "Single request should succeed")

        results = self._load_json(results_dir)
        self.assertEqual(results['ALL STATS']['Sets']['Count'], 1)


if __name__ == '__main__':
    unittest.main()
