import { test, expect } from '../setup/fixtures';

// Define all environments from .env.example
const environments = [
  {
    name: 'localhost',
    url: process.env.CYPRESS_TEST_URL_LOCALHOST || 'http://cyber24:3000',
    email: process.env.CYPRESS_TEST_EMAIL_LOCALHOST || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_LOCALHOST || 'your_preprod_password'
  },
  {
    name: 'akabox',
    url: process.env.CYPRESS_TEST_URL_AKABOX || 'https://ai.tail22dc1.ts.net',
    email: process.env.CYPRESS_TEST_EMAIL_AKABOX || 'test@test',
    password: process.env.CYPRESS_TEST_PASSWORD_AKABOX || 'your_preprod_password'
  },
  {
    name: 'akaboxdot',
    url: process.env.CYPRESS_TEST_URL_AKABOXDOT || 'https://akabox.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_AKABOXDOT || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_AKABOXDOT || 'your_preprod_password'
  },
  {
    name: 'preprod',
    url: process.env.CYPRESS_TEST_URL_PREPROD || 'https://preprod.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_PREPROD || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_PREPROD || 'your_preprod_password'
  },
  {
    name: 'axontech',
    url: process.env.CYPRESS_TEST_URL_AXONTECH || 'https://axontech.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_AXONTECH || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_AXONTECH || 'your_axontech_password'
  },
  {
    name: 'chat',
    url: process.env.CYPRESS_TEST_URL_CHAT || 'https://chat.privatestack.ai',
    email: process.env.CYPRESS_TEST_EMAIL_CHAT || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_CHAT || 'your_chat_password'
  },
  {
    name: 'dex',
    url: process.env.CYPRESS_TEST_URL_DEX || 'https://digifi-gpt.digitalist.tools',
    email: process.env.CYPRESS_TEST_EMAIL_DEX || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_DEX || 'your_dex_password'
  },
  {
    name: 'dotab',
    url: process.env.CYPRESS_TEST_URL_DOTAB || 'https://gpt.digitalist.tools',
    email: process.env.CYPRESS_TEST_EMAIL_DOTAB || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_DOTAB || 'your_dotab_password'
  },
  {
    name: 'granges',
    url: process.env.CYPRESS_TEST_URL_GRANGES || 'https://granges.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_GRANGES || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_GRANGES || 'your_granges_password'
  },
  {
    name: 'grow',
    url: process.env.CYPRESS_TEST_URL_GROW || 'https://grow.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_GROW || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_GROW || 'your_grow_password'
  },
  {
    name: 'hsr',
    url: process.env.CYPRESS_TEST_URL_HSR || 'https://assistent.hsr.se/',
    email: process.env.CYPRESS_TEST_EMAIL_HSR || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_HSR || 'your_hsr_password'
  },
  {
    name: 'northbound',
    url: process.env.CYPRESS_TEST_URL_NORTHBOUND || 'https://northbound.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_NORTHBOUND || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_NORTHBOUND || 'your_northbound_password'
  },
  {
    name: 'resiliencebot',
    url: process.env.CYPRESS_TEST_URL_RESILIENCEBOT || 'https://resiliencebot.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_RESILIENCEBOT || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_RESILIENCEBOT || 'your_resiliencebot_password'
  },
  {
    name: 'sanda',
    url: process.env.CYPRESS_TEST_URL_SANDA || 'https://sanda.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_SANDA || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_SANDA || 'your_sanda_password'
  },
  {
    name: 'stage',
    url: process.env.CYPRESS_TEST_URL_STAGE || 'https://sanda.open-webui.dgstage.se',
    email: process.env.CYPRESS_TEST_EMAIL_STAGE || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_STAGE || 'your_stage_password'
  },
  {
    name: 'ur',
    url: process.env.CYPRESS_TEST_URL_UR || 'https://aiportalen.ur.se',
    email: process.env.CYPRESS_TEST_EMAIL_UR || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_UR || 'your_ur_password'
  },
  {
    name: 'ekn',
    url: process.env.CYPRESS_TEST_URL_EKN || 'https://ai.ekn.se',
    email: process.env.CYPRESS_TEST_EMAIL_EKN || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_EKN || 'your_ekn_password'
  },
  {
    name: 'msb',
    url: process.env.CYPRESS_TEST_URL_MSB || 'https://msb.open-webui.dgstage.se/',
    email: process.env.CYPRESS_TEST_EMAIL_MSB || 'admin@digitalist.cloud',
    password: process.env.CYPRESS_TEST_PASSWORD_MSB || 'your_msb_password'
  }
];

test.describe('Smoke Test', () => {
  test.setTimeout(120000); // 2 minutes timeout per test

  test('should login and verify basic functionality', async ({ browser, page }) => {
    // Get the baseURL from the current context
    const baseURL = page.context()._options?.baseURL || 'http://localhost:3000';

    // Find the matching environment
    const env = environments.find(e => e.url === baseURL);
    if (!env) {
      throw new Error(`No environment found for baseURL: ${baseURL}`);
    }

    try {
      await page.goto('/');

      const currentURL = page.url();
      if (currentURL.includes('/auth') || currentURL.includes('login')) {
        await page.getByLabel('Email').fill(env.email);
        await page.locator('input[type="password"]').fill(env.password);
        await page.getByRole('button', { name: /sign in/i }).click();

        try {
          await Promise.race([
            page.waitForURL('**/', { timeout: 15000 }),
            page.waitForSelector('[data-testid="user-menu"]', { timeout: 15000 }),
            page.waitForSelector('[aria-label="User Menu"]', { timeout: 15000 }),
            page.waitForSelector('button:has-text("User Menu")', { timeout: 15000 })
          ]);
        } catch (e) {
          const errorLocator = page.locator('text=/error|invalid|failed/i');
          const hasError = await errorLocator.isVisible().catch(() => false);
          if (hasError) {
            throw new Error(`Login failed for ${env.name}: ${await errorLocator.textContent()}`);
          }
        }
      }

      const mainPageIndicators = [
        page.locator('[data-testid="chat-input"]'),
        page.locator('textarea[placeholder*="message"]'),
        page.locator('button:has-text("Send")'),
        page.locator('[aria-label="Send message"]'),
        page.locator('text=/chat|conversation|message/i')
      ];

      let mainPageFound = false;
      for (const indicator of mainPageIndicators) {
        try {
          await expect(indicator).toBeVisible({ timeout: 5000 });
          mainPageFound = true;
          break;
        } catch (e) {
          continue;
        }
      }

      if (!mainPageFound) {
        const navElements = [
          page.locator('nav'),
          page.locator('[role="navigation"]'),
          page.locator('text=/dashboard|home|welcome/i'),
          page.locator('h1, h2, h3')
        ];

        for (const nav of navElements) {
          try {
            await expect(nav).toBeVisible({ timeout: 3000 });
            mainPageFound = true;
            break;
          } catch (e) {
            continue;
          }
        }
      }

      if (!mainPageFound) {
        throw new Error(`Could not verify main page load for ${env.name}`);
      }

      console.log(`✓ Smoke test passed for ${env.name} (${env.url})`);

    } catch (error) {
      console.error(`✗ Smoke test failed for ${env.name} (${env.url}):`, error);
      throw error;
    }
  });
});