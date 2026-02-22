

class KeyCodec:
    HLC_LEN_SIZE: int = 2
    USER_LEN_SIZE: int = 2
    SENTINEL: bytes = b""
    TOMBSTONE: bytes = b""

    @classmethod
    def hlc_len(cls, hlc_bytes: bytes) -> bytes:
        return len(hlc_bytes).to_bytes(cls.HLC_LEN_SIZE, "big")

    @classmethod
    def decrement_key(cls, key: bytes) -> bytes:
        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0x00:
                return key[:i] + bytes([key[i] - 1]) + b"\xFF" * (len(key) - i - 1)
        return b""

    @classmethod
    def increment_key(cls, key: bytes) -> bytes:
        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0xFF:
                return key[:i] + bytes([key[i] + 1])
        return key + b"\x00"

    @classmethod
    def data_prefix(cls, keyspace: bytes, user_key: bytes) -> bytes:
        user_len = len(user_key).to_bytes(cls.USER_LEN_SIZE, "big")
        return keyspace + user_len + user_key

    @classmethod
    def index_prefix(cls, keyspace: bytes, hlc_bytes: bytes) -> bytes:
        hlc_len = cls.hlc_len(hlc_bytes)
        return keyspace + hlc_len + hlc_bytes

    @classmethod
    def data_key(cls, keyspace: bytes, user_key: bytes, hlc_bytes: bytes) -> bytes:
        hlc_len = cls.hlc_len(hlc_bytes)
        return cls.data_prefix(keyspace, user_key) + hlc_len + hlc_bytes

    @classmethod
    def index_key(cls, keyspace: bytes, user_key: bytes, hlc_bytes: bytes) -> bytes:
        user_len = len(user_key).to_bytes(cls.USER_LEN_SIZE, "big")
        return cls.index_prefix(keyspace, hlc_bytes) + user_len + user_key

    @classmethod
    def parse_data_key(
        cls,
        keyspace: bytes,
        data_key: bytes
    ) -> tuple[bytes, bytes] | None:
        """
        Parse:
            data_key = keyspace
                     || len(user_key:2)
                     || user_key
                     || len(hlc:2)
                     || hlc_bytes

        Returns:
            (hlc_bytes, user_key)
        or:
            None if corrupted / truncated
        """
        base = len(keyspace)

        if not data_key.startswith(keyspace):
            return None

        if len(data_key) < base + cls.USER_LEN_SIZE:
            return None

        user_len = int.from_bytes(data_key[base: base + cls.USER_LEN_SIZE], "big")
        pos = base + cls.USER_LEN_SIZE

        if len(data_key) < pos + user_len + cls.HLC_LEN_SIZE:
            return None

        user_key = data_key[pos: pos + user_len]
        pos += user_len

        hlc_len = int.from_bytes(data_key[pos: pos + cls.HLC_LEN_SIZE], "big")
        pos += cls.HLC_LEN_SIZE

        if len(data_key) < pos + hlc_len:
            return None

        hlc_bytes = data_key[pos: pos + hlc_len]

        return hlc_bytes, user_key

    @classmethod
    def parse_index_key(
        cls,
        keyspace: bytes,
        index_key: bytes
    ) -> tuple[bytes, bytes] | None:
        """
        Parse:
            index_key = keyspace || len(hlc:2) || hlc_bytes || len(user_key:2) || user_key

        Returns:
            (hlc_bytes, user_key)
        or:
            None if corrupted / truncated
        """
        base = len(keyspace)

        if not index_key.startswith(keyspace):
            return None

        if len(index_key) < base + cls.HLC_LEN_SIZE:
            return None

        hlc_len = int.from_bytes(index_key[base: base + cls.HLC_LEN_SIZE], "big")
        pos = base + cls.HLC_LEN_SIZE

        if len(index_key) < pos + hlc_len + cls.USER_LEN_SIZE:
            return None

        hlc_bytes = index_key[pos: pos + hlc_len]
        pos += hlc_len

        user_len = int.from_bytes(index_key[pos: pos + cls.USER_LEN_SIZE], "big")
        pos += cls.USER_LEN_SIZE

        if len(index_key) < pos + user_len:
            return None

        user_key = index_key[pos: pos + user_len]

        return hlc_bytes, user_key
