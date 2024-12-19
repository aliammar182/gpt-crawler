import { Configuration, PlaywrightCrawler, downloadListOfUrls } from "crawlee";
import { glob } from "glob";
import { Config, configSchema } from "./config.js";
import { Page } from "playwright";
import { isWithinTokenLimit } from "gpt-tokenizer";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
import { readFile } from "fs/promises";

dotenv.config();

// Initialize S3 client
const s3Client = new S3Client({ region: process.env.AWS_REGION });

let pageCounter = 0;
let crawler: PlaywrightCrawler;

// Function to extract page HTML
export function getPageHtml(page: Page, selector = ".article-content") {
  return page.evaluate((selector) => {
    let el: HTMLElement | null = null;

    if (selector.startsWith("/")) {
      const elements = document.evaluate(
        selector,
        document,
        null,
        XPathResult.ANY_TYPE,
        null
      );
      el = elements.iterateNext() as HTMLElement | null;
    } else {
      el = document.querySelector(selector);
      if (!el) {
        el = document.querySelector("[xpath]");
      }
    }

    return el?.innerText || "";
  }, selector);
}

// Function to wait for XPath
export async function waitForXPath(page: Page, xpath: string, timeout: number) {
  await page.waitForFunction(
    (xpath) => {
      const elements = document.evaluate(
        xpath,
        document,
        null,
        XPathResult.ANY_TYPE,
        null
      );
      return elements.iterateNext() !== null;
    },
    xpath,
    { timeout }
  );
}

// Crawl function
export async function crawl(config: Config) {
  configSchema.parse(config);

  if (process.env.NO_CRAWL !== "true") {
    crawler = new PlaywrightCrawler({
      async requestHandler({ request, page, enqueueLinks, log, pushData }) {
        const title = await page.title();
        pageCounter++;
        log.info(
          `Crawling: Page ${pageCounter} / ${config.maxPagesToCrawl} - URL: ${request.loadedUrl}...`
        );

        if (config.selector) {
          if (config.selector.startsWith("/")) {
            await waitForXPath(
              page,
              config.selector,
              config.waitForSelectorTimeout ?? 25000
            );
          } else {
            try {
              await page.waitForSelector(config.selector, {
                state: "visible",
                timeout: config.waitForSelectorTimeout ?? 25000
              });
            } catch (error) {
              log.info(`Failed to find selector ${config.selector} on ${request.loadedUrl}`);
              return;
            }
          }
        }

        const html = await getPageHtml(page, config.selector);

        if (!html) {
          log.info(`No content found for selector ${config.selector} on ${request.loadedUrl}`);
          return;
        }

        await pushData({ title, url: request.loadedUrl, html });

        if (config.onVisitPage) {
          await config.onVisitPage({ page, pushData });
        }

        await enqueueLinks({
          globs: typeof config.match === "string" ? [config.match] : config.match,
          exclude: typeof config.exclude === "string" ? [config.exclude] : config.exclude ?? []
        });
      },
      maxRequestsPerCrawl: config.maxPagesToCrawl,
      preNavigationHooks: [
        async ({ request, page, log }) => {
          const RESOURCE_EXCLUSIONS = config.resourceExclusions ?? [];
          if (RESOURCE_EXCLUSIONS.length === 0) {
            return;
          }
          if (config.cookie) {
            const cookies = (
              Array.isArray(config.cookie) ? config.cookie : [config.cookie]
            ).map((cookie) => {
              return {
                name: cookie.name,
                value: cookie.value,
                url: request.loadedUrl
              };
            });
            await page.context().addCookies(cookies);
          }
          await page.route(
            `**/*.{${RESOURCE_EXCLUSIONS.join()}}`,
            (route) => route.abort("aborted")
          );
          log.info(`Aborting requests for as this is a resource excluded route`);
        }
      ]
    });

    const isUrlASitemap = /sitemap.*\.xml$/.test(config.url);

    if (isUrlASitemap) {
      const listOfUrls = await downloadListOfUrls({ url: config.url });
      await crawler.addRequests(listOfUrls);
      await crawler.run();
    } else {
      await crawler.run([config.url]);
    }
  }
}

// Write function with S3 integration
export async function write(config: Config) {
  const jsonFiles = await glob("storage/datasets/default/*.json", {
    absolute: true
  });

  console.log(`Found ${jsonFiles.length} files to combine...`);

  let currentResults: Record<string, any>[] = [];
  let currentSize: number = 0;
  let fileCounter: number = 1;
  const maxBytes: number = config.maxFileSize
    ? config.maxFileSize * 1024 * 1024
    : Infinity;

  const getStringByteSize = (str: string): number =>
    Buffer.byteLength(str, "utf-8");

  const getS3Key = (): string =>
    `${config.outputFileName.replace(/\.json$/, "")}-${fileCounter}.json`;

  const uploadToS3 = async (key: string, content: string) => {
    const bucketName = process.env.S3_BUCKET_NAME;
    if (!bucketName) {
      throw new Error("S3_BUCKET_NAME is not defined in the environment variables.");
    }

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: content,
      ContentType: "application/json"
    });

    try {
      await s3Client.send(command);
      console.log(`Uploaded ${key} to S3 bucket ${bucketName}`);
    } catch (error) {
      console.error(`Failed to upload ${key}:`, error);
      throw error;
    }
  };

  const writeBatchToFile = async (): Promise<void> => {
    const key = getS3Key();
    const content = JSON.stringify(currentResults, null, 2);
    await uploadToS3(key, content);
    currentResults = [];
    currentSize = 0;
    fileCounter++;
  };

  let estimatedTokens: number = 0;

  const addContentOrSplit = async (
    data: Record<string, any>
  ): Promise<void> => {
    const contentString: string = JSON.stringify(data);
    const tokenCount: number | false = isWithinTokenLimit(
      contentString,
      config.maxTokens || Infinity
    );

    if (typeof tokenCount === "number") {
      if (estimatedTokens + tokenCount > config.maxTokens!) {
        if (currentResults.length > 0) {
          await writeBatchToFile();
        }
        estimatedTokens = Math.floor(tokenCount / 2);
        currentResults.push(data);
      } else {
        currentResults.push(data);
        estimatedTokens += tokenCount;
      }
    }

    currentSize += getStringByteSize(contentString);
    if (currentSize > maxBytes) {
      await writeBatchToFile();
    }
  };

  for (const file of jsonFiles) {
    const fileContent = await readFile(file, "utf-8");
    const data: Record<string, any> = JSON.parse(fileContent);
    await addContentOrSplit(data);
  }

  if (currentResults.length > 0) {
    await writeBatchToFile();
  }

  return "Upload complete.";
}

// GPTCrawlerCore class
class GPTCrawlerCore {
  config: Config;

  constructor(config: Config) {
    this.config = config;
  }

  async crawl() {
    await crawl(this.config);
  }

  async write(): Promise<string> {
    return write(this.config);
  }
}

export default GPTCrawlerCore;
