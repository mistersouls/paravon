import math
import pytest

from paravon.core.cluster.phi import FailureDetector


@pytest.mark.ut
def test_initial_state():
    fd = FailureDetector()
    assert fd.compute_phi() == float("inf")
    assert fd.is_suspect() is True
    assert fd.is_alive() is False


@pytest.mark.ut
def test_compute_phi_zero_intervals():
    fd = FailureDetector()
    fd.record_heartbeat(100.0)
    assert fd.compute_phi(101.0) == 0.0


@pytest.mark.ut
def test_compute_phi_basic():
    fd = FailureDetector()
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(101.0)
    fd.record_heartbeat(102.0)

    # inter-arrivals = [1, 1], mean = 1
    # delta = now - last = 103 - 102 = 1
    # x = -1 / 1 = -1
    # phi = -log10(exp(-1))
    expected = -math.log10(math.exp(-1))
    assert pytest.approx(fd.compute_phi(103.0), rel=1e-6) == expected


@pytest.mark.ut
def test_phi_grows_with_time():
    fd = FailureDetector()
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(101.0)

    phi1 = fd.compute_phi(102.0)
    phi2 = fd.compute_phi(110.0)

    assert phi2 > phi1


@pytest.mark.ut
def test_min_samples_blocks_alive():
    fd = FailureDetector()
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(101.0)

    # min_samples = 5 → not enough
    assert fd.is_alive(min_samples=5) is False
    assert fd.is_suspect(min_samples=5) is True


@pytest.mark.ut
def test_min_samples_allows_alive():
    fd = FailureDetector()
    for t in [100, 101, 102, 103, 104, 105]:
        fd.record_heartbeat(t)

    # enough samples
    now = 106.0
    assert fd.is_alive(now=now, min_samples=3) is True
    assert fd.is_suspect(now=now, min_samples=3) is False


@pytest.mark.ut
def test_reset_clears_state():
    fd = FailureDetector()
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(101.0)
    fd.reset()

    assert fd.compute_phi() == float("inf")
    assert fd.is_alive() is False


@pytest.mark.ut
def test_min_interval_enforced():
    fd = FailureDetector(min_interval=0.5)
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(100.1)

    inter = fd.compute_phi(101.0)
    assert inter >= 0.0


@pytest.mark.ut
def test_max_interval_enforced():
    fd = FailureDetector(max_interval=1.0)
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(200.0)

    phi = fd.compute_phi(201.0)
    assert phi >= 0.0


@pytest.mark.ut
def test_phi_cap():
    fd = FailureDetector(phi_cap=5.0)
    fd.record_heartbeat(100.0)
    fd.record_heartbeat(101.0)

    phi = fd.compute_phi(1000.0)
    assert phi <= 5.0
