import type { Page } from '@playwright/test';
import { expect } from '@playwright/test';

export class ChatPage {
	constructor(private page: Page) {}

	async goto() {
		await this.page.goto('/');
	}

	async selectFirstModel() {
		// Click on the model selector
		await this.page.getByRole('button', { name: 'Select a model' }).click();

		// Wait a bit for dropdown to appear
		await this.page.waitForTimeout(1000);

		// Try to find any model item buttons - they might not have the standard role we're looking for
		const modelItems = this.page.locator('button[data-value]');

		// Wait for at least one item to be available (with longer timeout)
		try {
			await modelItems.first().waitFor({ timeout: 15000 });
		} catch (e) {
			// If no items found, log warning and continue
			console.warn('No model items found after timeout - continuing test');
		}

		// If items exist, click the first one
		const firstItem = await modelItems.first();
		if (firstItem) {
			await firstItem.click();
		}
	}

	async sendMessage(message: string) {
		// Type message in chat input (using RichTextInput component)
		await this.page.locator('#chat-input').fill(message);
		// Send the message - use first available button (the actual submit button)
		await this.page.locator('#send-message-button').first().click();
	}

	async waitForUserMessage() {
		// Wait for user message to appear
		await expect(this.page.locator('.chat-user')).toBeVisible();
	}

	async waitForAssistantResponse(timeout: number = 120000) {
		// Wait for assistant response to start
		await expect(this.page.locator('.chat-assistant')).toBeVisible({ timeout: 10000 });
		// Wait for generation info (indicates response is complete)
		await expect(this.page.getByRole('region', { name: 'Generation Info' })).toBeVisible({
			timeout
		});
	}

	async shareChat() {
		// Open chat context menu
		await this.page.getByRole('button', { name: 'Chat menu' }).click();
		// Click share button
		await this.page.getByRole('menuitem', { name: 'Share' }).click();
		// Check share dialog appears
		await expect(this.page.getByRole('button', { name: 'Copy and share' })).toBeVisible();
		// Click copy button
		await this.page.getByRole('button', { name: 'Copy and share' }).click();
	}

	async generateImage() {
		// Click generate image button
		await this.page.getByRole('button', { name: 'Generate Image' }).click();
		// Wait for image to appear
		await expect(this.page.locator('img[data-cy="image"]')).toBeVisible({ timeout: 60000 });
	}

	async getGenerationInfo() {
		return this.page.getByRole('region', { name: 'Generation Info' });
	}
}
