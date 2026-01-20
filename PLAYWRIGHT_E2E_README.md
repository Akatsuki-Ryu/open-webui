# Open WebUI Playwright E2E Tests

This document provides comprehensive instructions for running Playwright End-to-End (E2E) tests for Open WebUI.

## Overview

The test suite covers core user functionality including:
- **Chat**: Model selection, text messaging, chat sharing, and image generation
- **Documents**: Document upload, search, and management
- **Authentication**: User login/registration flows

## Prerequisites

### System Requirements
- Node.js 18+ (tested with v22.9.0)
- npm 6+
- Open WebUI application running locally
- Chromium browser (automatically installed by Playwright)

### Application Setup
Ensure Open WebUI is running locally on `http://localhost:8080` with:
- Backend API accessible
- Frontend serving on port 8080
- Database properly configured

## Installation

The Playwright dependencies are already configured in `package.json`. If needed, install dependencies:

```bash
npm install
```

Playwright browsers are pre-installed in the project.

## Test Configuration

### Configuration Files
- `playwright.config.ts` - Main Playwright configuration
- `tests/e2e/setup/fixtures.ts` - Test fixtures and authentication helpers
- `tests/e2e/setup/global-setup.ts` - Global test setup

### Key Configuration Details
- **Base URL**: `http://localhost:8080`
- **Browser**: Chromium only
- **Timeouts**: 60s global, 10s expect
- **Screenshots**: On failure only
- **Videos**: Retained on failure
- **Parallel Execution**: Enabled

## Running Tests

### Quick Start
```bash
# Run all E2E tests
npm run test:e2e
```

### Specific Test Commands

```bash
# Run tests in headed mode (visible browser)
npm run test:e2e:headed

# Run Playwright UI mode (interactive test runner)
npm run test:e2e:ui

# Run tests in debug mode
npm run test:e2e:debug

# Run specific test file
npx playwright test tests/e2e/specs/chat.spec.ts

# Run tests with specific browser
npx playwright test --project=chromium

# Run tests on mobile viewport
npx playwright test --project="Mobile Chrome"
```

### Test Filtering

```bash
# Run tests matching pattern
npx playwright test --grep "chat"

# Run specific test
npx playwright test --grep "user can perform text chat"

# Run tests in specific file
npx playwright test chat.spec.ts
```

## Test Structure

```
tests/e2e/
├── setup/
│   ├── fixtures.ts          # Authentication fixtures
│   ├── global-setup.ts      # Global test setup
│   └── pages/               # Page Object Models
│       ├── ChatPage.ts
│       └── DocumentsPage.ts
├── specs/
│   ├── chat.spec.ts         # Chat functionality tests
│   └── documents.spec.ts    # Document management tests
└── playwright.config.ts     # Playwright configuration
```

## Test Data & Setup

### Admin User
Tests use a pre-configured admin user:
- **Email**: `admin@example.com`
- **Password**: `password`
- **Name**: `Admin User`

The admin user is automatically registered during test setup if it doesn't exist.

### Database Requirements
Tests expect the existing database setup with:
- User authentication system
- Chat functionality
- Document storage capabilities

## Troubleshooting

### Common Issues

#### 1. Tests Can't Connect to Application
**Error**: `net::ERR_CONNECTION_REFUSED` or timeout errors
**Solution**:
- Ensure Open WebUI is running on `http://localhost:8080`
- Check if the development server is started: `npm run dev:5050`
- Verify no firewall blocks are active

#### 2. Authentication Failures
**Error**: Login tests failing
**Solution**:
- Ensure database is clean/reset before running tests
- Check that the admin user can be registered
- Verify API endpoints are accessible

#### 3. Element Not Found Errors
**Error**: `Locator not found` or selector errors
**Solution**:
- UI changes may have affected selectors
- Update Page Object Models in `tests/e2e/setup/pages/`
- Check if the application is fully loaded before tests run

#### 4. Timeout Errors
**Error**: Tests timing out
**Solution**:
- Increase timeout values in `playwright.config.ts`
- Check system performance and available resources
- Ensure LLM responses are not taking too long

### Debug Mode

Run tests in debug mode to step through execution:

```bash
npm run test:e2e:debug
```

This opens an interactive debugger where you can:
- Step through test execution
- Inspect page state
- Check network requests
- View console logs

### Generating Test Reports

After test execution, view reports:

```bash
# HTML report (opens in browser)
npx playwright show-report

# JSON results
npx playwright show-report test-results/results.json
```

## CI/CD Integration

For future CI/CD integration, add to your pipeline:

```yaml
- name: Install Playwright
  run: npm install

- name: Run E2E tests
  run: npm run test:e2e
  env:
    CI: true
```

## Best Practices

### Writing Tests
- Use Page Object Models for maintainable selectors
- Keep tests focused on user workflows
- Use descriptive test names
- Handle async operations properly
- Clean up test data when possible

### Debugging Tests
- Use `page.pause()` for interactive debugging
- Check screenshots/videos on failure
- Use `console.log()` for debugging values
- Leverage Playwright's trace viewer

### Maintenance
- Update Page Object Models when UI changes
- Keep test data realistic but minimal
- Regularly review and update flaky tests
- Monitor test execution times

## Migration from Cypress

This Playwright setup replaces the previous Cypress E2E tests with:
- ✅ Migrated chat functionality tests
- ✅ Migrated authentication setup
- ✅ Improved test reliability and speed
- ✅ Better debugging capabilities
- ✅ Native mobile testing support
- ✅ Parallel test execution

## Support

For issues with Playwright tests:
1. Check this documentation first
2. Review Playwright's official documentation
3. Check application logs for API errors
4. Use debug mode to isolate issues

---

**Last Updated**: January 2026
**Test Framework**: Playwright v1.57.0
**Browser**: Chromium
**Base URL**: http://localhost:8080