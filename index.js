const {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectCommand,
} = require("@aws-sdk/client-s3");
const cron = require("node-cron");
const fs = require("fs");

const s3Client = new S3Client({
  region: process.env.S3_UPLOAD_REGION,
  credentials: {
    accessKeyId: process.env.S3_UPLOAD_KEY,
    secretAccessKey: process.env.S3_UPLOAD_SECRET,
  },
});

function logToFile(logType, message) {
  const filename = logType === "ERROR" ? "error.log" : "deletion.log";
  const logMessage = `[${new Date().toISOString()}] [${logType}] ${message}\n`;
  fs.appendFileSync(filename, logMessage);
}

// Function to delete files older than specified days in a folder
async function deleteFilesInFolder(bucketName, folderPrefix, maxAgeDays) {
  try {
    const listObjectsParams = {
      Bucket: bucketName,
      Prefix: folderPrefix,
    };

    const objects = await s3Client.send(
      new ListObjectsV2Command(listObjectsParams)
    );

    for (const object of objects.Contents) {
      try {
        const objectKey = object.Key;
        const objectAgeInDays = Math.ceil(
          (new Date() - object.LastModified) / (1000 * 60 * 60 * 24)
        );

        console.log(objectAgeInDays, maxAgeDays);
        if (
          objectKey !== folderPrefix &&
          !objectKey.endsWith("/") &&
          object.Size > 0 &&
          objectAgeInDays >= maxAgeDays
        ) {
          console.log(`Deleting file: ${objectKey}`);
          await s3Client.send(
            new DeleteObjectCommand({
              Bucket: bucketName,
              Key: objectKey,
            })
          );
          console.log(`Deleted file: ${objectKey}`);
          logToFile("INFO", `Deleted file: ${objectKey}`);
        }
      } catch (error) {
        console.error(`Error processing file ${object.Key}:`, error);

        // Log error to the file
        logToFile("ERROR", `Error processing file ${object.Key}: ${error}`);

        // Continue to the next iteration
        continue;
      }
    }
  } catch (error) {
    console.error("Error listing objects:", error);

    // Log the error to a separate file
    logToFile("ERROR", `Error listing objects: ${error}`);
  }
}

// Schedule periodic tasks for each folder
// Run daily at midnight 0:00
deleteFilesInFolder(process.env.S3_UPLOAD_BUCKET, "plan_1/", 1);

cron.schedule("0 0 * * *", () => {
  // Delete files older than 7 days in the plan_1 folder
  deleteFilesInFolder(process.env.S3_UPLOAD_BUCKET, "plan_1/", 1);

  // Delete files older than 30 days in the plan_2 folder
  deleteFilesInFolder(process.env.S3_UPLOAD_BUCKET, "plan_2/", 30);

  // Delete files older than 90 days in the plan_3 folder
  deleteFilesInFolder(process.env.S3_UPLOAD_BUCKET, "plan_3/", 90);
});

console.log("Script is running...");
