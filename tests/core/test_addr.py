import pytest
from unittest.mock import Mock

from paravon.core.transport.addr import get_remote_addr, get_local_addr


@pytest.mark.ut
def test_get_remote_addr_with_socket():
    transport = Mock()
    sock = Mock()
    sock.getpeername.return_value = ("1.2.3.4", 5678)
    transport.get_extra_info.side_effect = lambda key: sock if key == "socket" else None

    assert get_remote_addr(transport) == ("1.2.3.4", 5678)


@pytest.mark.ut
def test_get_remote_addr_with_peername():
    transport = Mock()
    transport.get_extra_info.side_effect = lambda key: None if key == "socket" else ("5.6.7.8", 9999)

    assert get_remote_addr(transport) == ("5.6.7.8", 9999)


@pytest.mark.ut
def test_get_remote_addr_invalid():
    transport = Mock()

    # Simule: pas de socket → fallback sur peername
    def fake_extra_info(key):
        if key == "socket":
            return None
        if key == "peername":
            return ("only-one-element",)
        return None

    transport.get_extra_info.side_effect = fake_extra_info

    assert get_remote_addr(transport) is None


@pytest.mark.ut
def test_get_local_addr_with_socket():
    transport = Mock()
    sock = Mock()
    sock.getsocketname.return_value = ("127.0.0.1", 1234)
    transport.get_extra_info.side_effect = lambda key: sock if key == "socket" else None

    assert get_local_addr(transport) == ("127.0.0.1", 1234)


@pytest.mark.ut
def test_get_local_addr_with_sockname():
    transport = Mock()
    transport.get_extra_info.side_effect = lambda key: None if key == "socket" else ("10.0.0.1", 8080)

    assert get_local_addr(transport) == ("10.0.0.1", 8080)


@pytest.mark.ut
def test_get_local_addr_invalid():
    transport = Mock()

    def fake_extra_info(key):
        if key == "socket":
            return None
        if key == "sockname":
            return ("weird",)
        return None

    transport.get_extra_info.side_effect = fake_extra_info

    assert get_local_addr(transport) is None

