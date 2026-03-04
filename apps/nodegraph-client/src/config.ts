export const config = {
  ws: {
    protocol: 'ws://',
    host: 'localhost',
    upPort: 4225,
    downPort: 4226,
    upPath: '/command',
    downPath: '/update',
  },
  api: {
    timelineUrl: 'http://localhost:9090/timeline',
  },
} as const;