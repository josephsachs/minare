/**
 * Enhanced monitoring script for Artillery test results
 * Provides real-time metrics and saves historical data
 *
 * Usage: node improved-monitor.js [path/to/artillery/report-file.json] [--save-history]
 */

const fs = require('fs');
const path = require('path');
const os = require('os');

// Process command line arguments
let reportFile = './artillery-report.json';
let saveHistory = false;

process.argv.forEach((arg, index) => {
  if (index >= 2) {
    if (arg === '--save-history') {
      saveHistory = true;
    } else if (!arg.startsWith('--')) {
      reportFile = arg;
    }
  }
});

// History tracking
const history = {
  timestamps: [],
  responseTimeMean: [],
  responseTimeP95: [],
  responseTimeP99: [],
  requestsPerSecond: [],
  successRate: [],
  concurrentUsers: []
};

// ANSI color codes for terminal output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  bg: {
    red: '\x1b[41m',
    green: '\x1b[42m',
    yellow: '\x1b[43m',
    blue: '\x1b[44m'
  }
};

// System information
const systemInfo = {
  cpu: os.cpus().length,
  totalMem: Math.round(os.totalmem() / (1024 * 1024 * 1024) * 10) / 10, // GB
  platform: os.platform(),
  release: os.release()
};

console.log(`${colors.bright}${colors.cyan}Enhanced Artillery Test Monitor${colors.reset}`);
console.log(`Monitoring report file: ${reportFile}`);
console.log(`Save history: ${saveHistory ? 'Yes' : 'No'}\n`);

console.log(`${colors.dim}System Information:${colors.reset}`);
console.log(`  CPU: ${systemInfo.cpu} cores`);
console.log(`  Memory: ${systemInfo.totalMem} GB`);
console.log(`  Platform: ${systemInfo.platform} (${systemInfo.release})\n`);

console.log(`Press Ctrl+C to exit\n`);

// Previous stats for change calculation
let prevStats = null;
let testStartTime = null;

// Update interval (2 seconds)
const updateInterval = 2000;

// Simple moving average calculation
function calculateMovingAverage(array, windowSize) {
  if (array.length < windowSize) return array[array.length - 1] || 0;

  const window = array.slice(-windowSize);
  return window.reduce((sum, val) => sum + val, 0) / windowSize;
}

// Generate a progress bar
function progressBar(value, maxValue, width = 20) {
  const percentage = Math.min(100, Math.floor((value / maxValue) * 100));
  const filledWidth = Math.floor((percentage / 100) * width);
  const emptyWidth = width - filledWidth;

  let color = colors.green;
  if (percentage > 90) color = colors.red;
  else if (percentage > 70) color = colors.yellow;

  const filled = color + '█'.repeat(filledWidth) + colors.reset;
  const empty = '░'.repeat(emptyWidth);

  return `${filled}${empty} ${percentage}%`;
}

// Main monitoring function
function monitorReport() {
  try {
    // Check if file exists
    if (!fs.existsSync(reportFile)) {
      console.log(`${colors.yellow}Waiting for report file to be created...${colors.reset}`);
      return;
    }

    // Read report file
    const reportData = fs.readFileSync(reportFile, 'utf8');

    // Parse JSON
    let report;
    try {
      report = JSON.parse(reportData);
    } catch (e) {
      console.log(`${colors.yellow}Report file exists but is not valid JSON yet. Waiting...${colors.reset}`);
      return;
    }

    // Extract latest stats
    if (!report.aggregate) {
      console.log(`${colors.yellow}Report exists but no aggregate data yet. Waiting...${colors.reset}`);
      return;
    }

    const stats = report.aggregate;

    // Set start time if this is the first data we've seen
    if (!testStartTime && report.start) {
      testStartTime = new Date(report.start);
    }

    // Clear terminal and show header
    console.clear();
    console.log(`${colors.bright}${colors.cyan}Enhanced Artillery Test Monitor - Live Stats${colors.reset}`);
    console.log(`Report file: ${reportFile}`);
    console.log(`Last update: ${new Date().toLocaleTimeString()}\n`);

    // Display current phase info
    let currentPhase = "Unknown";
    let phaseProgress = 0;
    let totalPhases = 0;

    if (report.phases && report.phases.length > 0) {
      totalPhases = report.phases.length;

      // Find current phase
      let elapsedTime = 0;
      if (testStartTime) {
        elapsedTime = (new Date() - testStartTime) / 1000;
      }

      let accumulatedDuration = 0;
      for (let i = 0; i < report.phases.length; i++) {
        const phase = report.phases[i];
        accumulatedDuration += phase.duration;

        if (elapsedTime <= accumulatedDuration) {
          currentPhase = phase.name || `Phase ${i+1}`;
          const phaseStart = accumulatedDuration - phase.duration;
          phaseProgress = (elapsedTime - phaseStart) / phase.duration;
          break;
        }
      }
    }

    // Calculate time elapsed
    let elapsedMs = 0;
    if (report.start && report.intermediate && report.intermediate.length > 0) {
      const startTime = new Date(report.start);
      const latestTimepoint = report.intermediate[report.intermediate.length - 1];
      const latestTime = new Date(latestTimepoint.timestamp);
      elapsedMs = latestTime - startTime;
    }

    const elapsedMinutes = Math.floor(elapsedMs / 60000);
    const elapsedSeconds = Math.floor((elapsedMs % 60000) / 1000);

    console.log(`${colors.bright}Test Progress:${colors.reset}`);
    console.log(`  Phase: ${colors.bright}${currentPhase}${colors.reset} (${totalPhases > 0 ? `${Math.floor(phaseProgress * 100)}%` : 'Unknown'})`);
    if (totalPhases > 0) {
      console.log(`  Phase progress: ${progressBar(phaseProgress, 1)}`);
    }
    console.log(`  Test Time: ${elapsedMinutes}m ${elapsedSeconds}s\n`);

    // Request statistics
    console.log(`${colors.bright}${colors.green}Overall Statistics:${colors.reset}`);

    const requestsPerSecond = stats.rates && stats.rates['http.request_rate']
      ? stats.rates['http.request_rate']
      : 0;

    const successRate = stats.counters && stats.counters['http.responses.total'] > 0
      ? (1 - (stats.counters['http.responses.error'] || 0) / stats.counters['http.responses.total']) * 100
      : 100;

    const activeVirtualUsers = stats.counters ?
      (stats.counters['vusers.created'] || 0) - (stats.counters['vusers.completed'] || 0)
      : 0;

    console.log(`  Scenarios launched: ${stats.scenarios.created}`);
    console.log(`  Scenarios completed: ${stats.scenarios.completed}`);
    console.log(`  Requests completed: ${stats.counters['http.requests'] || 0}`);
    console.log(`  Active virtual users: ${activeVirtualUsers}`);
    console.log(`  Requests per second: ${requestsPerSecond.toFixed(2)}`);
    console.log(`  Success rate: ${successRate.toFixed(2)}%`);

    console.log(`\n${colors.bright}Response Times:${colors.reset}`);
    console.log(`  Min: ${stats.latency.min.toFixed(1)} ms`);
    console.log(`  Mean: ${stats.latency.mean.toFixed(1)} ms`);
    console.log(`  Median: ${stats.latency.median.toFixed(1)} ms`);
    console.log(`  p95: ${stats.latency.p95.toFixed(1)} ms`);
    console.log(`  p99: ${stats.latency.p99.toFixed(1)} ms`);
    console.log(`  Max: ${stats.latency.max.toFixed(1)} ms`);

    // Track history for charts
    if (saveHistory) {
      history.timestamps.push(new Date().toISOString());
      history.responseTimeMean.push(stats.latency.mean);
      history.responseTimeP95.push(stats.latency.p95);
      history.responseTimeP99.push(stats.latency.p99);
      history.requestsPerSecond.push(requestsPerSecond);
      history.successRate.push(successRate);
      history.concurrentUsers.push(activeVirtualUsers);
    }

    // Error statistics
    if (stats.errors && Object.keys(stats.errors).length > 0) {
      console.log(`\n${colors.bright}${colors.red}Errors:${colors.reset}`);

      let totalErrors = 0;
      for (const [errorType, count] of Object.entries(stats.errors)) {
        console.log(`  ${errorType}: ${count}`);
        totalErrors += count;
      }

      const errorRate = stats.counters && stats.counters['http.requests'] > 0
        ? (totalErrors / stats.counters['http.requests'] * 100).toFixed(2)
        : 0;

      console.log(`  Error rate: ${errorRate}% (${totalErrors} of ${stats.counters['http.requests'] || 0} requests)`);
    }

    // Show changes from previous check if available
    if (prevStats) {
      const newRequestsCompleted = (stats.counters['http.requests'] || 0) - (prevStats.counters['http.requests'] || 0);
      const newScenariosCreated = stats.scenarios.created - prevStats.scenarios.created;
      const meanLatencyChange = stats.latency.mean - prevStats.latency.mean;
      const p95LatencyChange = stats.latency.p95 - prevStats.latency.p95;

      console.log(`\n${colors.bright}${colors.magenta}Changes since last update:${colors.reset}`);
      console.log(`  New scenarios: ${colors.green}+${newScenariosCreated}${colors.reset}`);
      console.log(`  New requests: ${colors.green}+${newRequestsCompleted}${colors.reset}`);
      console.log(`  Mean latency: ${meanLatencyChange > 0 ? colors.red : colors.green}${meanLatencyChange > 0 ? '+' : ''}${meanLatencyChange.toFixed(1)} ms${colors.reset}`);
      console.log(`  p95 latency: ${p95LatencyChange > 0 ? colors.red : colors.green}${p95LatencyChange > 0 ? '+' : ''}${p95LatencyChange.toFixed(1)} ms${colors.reset}`);

      // Calculate and show moving averages
      if (history.responseTimeMean.length > 5) {
        const meanAvg = calculateMovingAverage(history.responseTimeMean, 5);
        const p95Avg = calculateMovingAverage(history.responseTimeP95, 5);
        const rpsAvg = calculateMovingAverage(history.requestsPerSecond, 5);

        console.log(`\n${colors.bright}${colors.blue}Trends (5-point moving average):${colors.reset}`);
        console.log(`  Mean latency: ${meanAvg.toFixed(1)} ms`);
        console.log(`  p95 latency: ${p95Avg.toFixed(1)} ms`);
        console.log(`  Requests/sec: ${rpsAvg.toFixed(2)}`);
      }
    }

    // Store current stats for next comparison
    prevStats = stats;

    console.log(`\n${colors.dim}Monitoring... (Press Ctrl+C to exit)${colors.reset}`);

  } catch (error) {
    console.error(`${colors.red}Error monitoring report:${colors.reset}`, error);
  }
}

// Run initial check
monitorReport();

// Set up interval for ongoing monitoring
const intervalId = setInterval(monitorReport, updateInterval);

// Save history data to JSON file when exiting
function saveHistoryToFile() {
  if (!saveHistory || history.timestamps.length === 0) return;

  try {
    const timestamp = new Date().toISOString().replace(/:/g, '-');
    const historyFile = `artillery-history-${timestamp}.json`;

    fs.writeFileSync(historyFile, JSON.stringify(history, null, 2));
    console.log(`\n${colors.green}Saved history data to ${historyFile}${colors.reset}`);
  } catch (error) {
    console.error(`\n${colors.red}Failed to save history:${colors.reset}`, error);
  }
}

// Handle clean exit
process.on('SIGINT', () => {
  clearInterval(intervalId);
  console.log(`\n${colors.yellow}Monitoring stopped.${colors.reset}`);

  saveHistoryToFile();

  process.exit(0);
});