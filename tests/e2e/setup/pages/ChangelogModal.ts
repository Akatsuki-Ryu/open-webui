import type { Page } from '@playwright/test';

/**
 * Class to handle UI elements related to the "What's New" changelog modal
 */
export class ChangelogModal {
	constructor(private page: Page) {}

	/**
	 * Dismiss the "What's New" modal if it's visible
	 * The modal has a button with text "Okay, Let's Go!"
	 */
	async dismiss() {
		try {
			// Look for the modal dismiss button
			const dismissButton = this.page.getByRole('button', {
				name: "Okay, Let's Go!"
			});

			// Check if the button exists and is visible
			if (await dismissButton.isVisible()) {
				await dismissButton.click();
				console.log('Dismissed "What\'s New" modal');
				// Wait briefly for the modal to close
				await this.page.waitForTimeout(500);
				return true;
			}

			console.log('No "What\'s New" modal found');
			return false;
		} catch (error) {
			// Modal not found or dismiss failed
			console.log('No "What\'s New" modal found or dismiss failed');
			return false;
		}
	}

	/**
	 * Check if the "What's New" modal is visible
	 */
	async isVisible(): Promise<boolean> {
		try {
			const dismissButton = this.page.getByRole('button', {
				name: "Okay, Let's Go!"
			});
			return await dismissButton.isVisible();
		} catch {
			return false;
		}
	}
}
