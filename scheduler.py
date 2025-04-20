import asyncio
import aiofiles
from kztime import get_unix_dt
from contextlib import suppress


class Scheduler:
    def __init__(self, func, time: int):
        self.func = func
        self.time = time
        self.is_started = False
        self._task = None
        self._filename = 'timestamp.txt'

    async def start(self):
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            from_timestamp = await self._get_and_update_timestamp()
            await self.func(from_timestamp)
            await asyncio.sleep(self.time)
    
    async def _get_and_update_timestamp(self) -> int:
        try:
            async with aiofiles.open(self._filename, 'r') as file:
                curr_timestamp = int(await file.read())
        except:
            curr_timestamp = int(get_unix_dt().timestamp())
        finally:
            timestamp = int(get_unix_dt().timestamp())
            async with aiofiles.open(self._filename, 'w') as file:
                await file.write(str(timestamp))
        return curr_timestamp
    

if __name__ == '__main__':
    s = Scheduler(lambda: print('test'), 23)
    asyncio.run(s._get_and_update_timestamp())