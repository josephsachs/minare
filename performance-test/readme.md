# Minare WebSocket Artillery Test

A simple artillery test for the Minare WebSocket API.

## Prerequisites

- Node.js 14+
- Artillery: `npm install -g artillery`
- WebSocket module: `npm install ws`

## Setup

1. Make sure the Minare server is running on `localhost:8080`
2. Install dependencies:

```bash
npm install ws
```

## Running the Test

Execute the artillery test:

```bash
artillery run minare-test.yml
```

## What the Test Does

This basic test:

1. Connects to the command socket at `/ws`
2. Obtains a connection ID
3. Connects to the update socket at `/ws/updates` with the connection ID
4. Sends a simple ping command
5. Tracks basic metrics like connection success and message flow

## Understanding the Output

Artillery will produce output showing:

- Number of virtual users created
- Connection success/failure rates
- Response times for WebSocket operations
- Any errors encountered during the test

## Next Steps

This is a foundational test that can be expanded to:

- Send real entity mutations
- Track entity version staleness
- Simulate multiple users at higher load
- Add custom metrics and reporting