import time
import asyncio


async def mult(a: int) -> int:
    await asyncio.sleep(1)
    return a * 2


async def async_function(n: int):
    tasks = [mult(i) for i in range(n)]
    values = await asyncio.gather(*tasks)
    return sorted(values)


# Define a normal function that calls the async function
# Define a normal function that calls the async function and measures the duration
def normal_function():
    start_time = time.time()  # Record the start time
    # Use asyncio.run to get the result from the async function
    result = asyncio.run(async_function(10))
    end_time = time.time()  # Record the end time
    duration = end_time - start_time  # Calculate the duration
    print(f"Duration: {duration} seconds")  # Print the duration
    return result


# Call the normal function
print(normal_function())
