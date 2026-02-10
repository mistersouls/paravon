import asyncio
import logging
from typing import Any, Coroutine


class TaskSpawner:
    """
    A lightweight helper for spawning and tracking background asyncio tasks.

    This utility centralizes task creation, error reporting, and lifecycle
    management. It ensures that:
    - all spawned tasks are tracked until completion
    - unhandled exceptions inside tasks are logged
    - completed tasks are automatically removed from the internal registry

    The class does not impose any scheduling policy; it simply delegates
    execution to the provided event loop.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._tasks: set[asyncio.Task[Any]] = set()
        self._logger = logging.getLogger("core.helpers.spawn")

    @property
    def remaining_tasks(self) -> int:
        """
        Return the number of tasks currently being tracked.

        This reflects tasks that have been spawned but have not yet completed.
        """
        return len(self._tasks)

    def on_done(self, task: asyncio.Task[Any]) -> None:
        """
        Callback executed when a spawned task completes.

        If the task raised an exception, it is logged. The task is then removed
        from the internal tracking set.
        """
        if ex := task.exception():
            self._logger.error(
                f"Error occurred in task {task.get_name()}: {str(ex)}",
                exc_info=ex
            )

        self._tasks.discard(task)

    def spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[Any]:
        """
        Spawn a coroutine as a background task and track its lifecycle.

        The task is registered, given a completion callback, and scheduled
        immediately on the provided event loop.
        """
        task = self._loop.create_task(coro)
        task.add_done_callback(self.on_done)
        self._tasks.add(task)
        return task
