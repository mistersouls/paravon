import asyncio


class FlowControl:
    """
    Cooperative flow‑control helper for asyncio TCP transports.

    This class models the writable state of a transport and provides
    an awaitable `drain()` method similar to the one available on
    asyncio StreamWriter.

    It is used by the Streamer to:
    - pause sending when the transport's buffer is full
    - resume sending when the transport becomes writable again
    - allow backpressure propagation to async producers
    """

    def __init__(self) -> None:
        self._writable = asyncio.Event()
        self._writable.set()
        self.write_paused = False

    async def drain(self) -> None:
        """Block until writing is allowed again."""
        await self._writable.wait()

    def pause_writing(self) -> None:
        """Mark the transport as non‑writable and block future drains."""
        self.write_paused = True
        self._writable.clear()

    def resume_writing(self) -> None:
        """Mark the transport as writable and wake blocked drains."""
        if self.write_paused:
            self.write_paused = False
            self._writable.set()
