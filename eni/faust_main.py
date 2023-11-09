import time

import faust

app = faust.App(
    'hello-world',
    broker='kafka://127.0.0.1:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('greetings')
reset_counter_topic = app.topic('reset_counter')
print_stats_topic = app.topic('print_stats')

counter = 0
ts = time.perf_counter()


@app.agent(greetings_topic)
async def greet(greetings):
    global counter
    async for greeting in greetings:
        counter += 1
        # print(greeting, counter)


@app.agent(reset_counter_topic)
async def reset_counter(messages):
    global counter
    global ts
    async for _ in messages:
        counter = 0
        ts = time.perf_counter()


@app.agent(print_stats_topic)
async def print_stats(messages):
    async for _ in messages:
        print("***", counter, time.perf_counter() - ts)
