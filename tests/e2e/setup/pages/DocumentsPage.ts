import { Page, expect } from '@playwright/test';

export class DocumentsPage {
  constructor(private page: Page) {}

  async goto() {
    await this.page.goto('/documents');
  }

  async uploadDocument(filePath: string, fileName: string) {
    // Click upload button or drop zone
    const fileInput = this.page.locator('input[type="file"]');
    await fileInput.setInputFiles(filePath);

    // Wait for upload to complete
    await expect(this.page.getByText(fileName)).toBeVisible({ timeout: 30000 });
  }

  async searchDocuments(query: string) {
    // Type in search input
    await this.page.getByPlaceholder('Search documents...').fill(query);
    // Wait for search results
    await this.page.waitForTimeout(1000);
  }

  async getDocumentList() {
    return this.page.locator('[data-testid="document-item"]');
  }

  async selectDocument(index: number = 0) {
    const documents = this.getDocumentList();
    await documents.nth(index).click();
  }

  async deleteDocument(index: number = 0) {
    const documents = this.getDocumentList();
    const document = documents.nth(index);

    // Click context menu (three dots)
    await document.locator('[aria-label="Document menu"]').click();

    // Click delete option
    await this.page.getByRole('menuitem', { name: 'Delete' }).click();

    // Confirm deletion
    await this.page.getByRole('button', { name: 'Delete' }).click();

    // Wait for document to be removed
    await expect(document).not.toBeVisible();
  }

  async waitForDocumentsToLoad() {
    // Wait for document list to be populated or show empty state
    await expect(
      this.page.locator('[data-testid="document-item"]').first().or(
        this.page.getByText('No documents found')
      )
    ).toBeVisible();
  }
}