import {
    CreateAutoPredictorCommand,
    CreateDatasetCommand,
    CreateDatasetGroupCommand,
    CreateDatasetImportJobCommand,
    CreateForecastCommand,
    CreateForecastExportJobCommand,
    CreatePredictorCommand,
    DescribeAutoPredictorCommand,
    DescribeDatasetImportJobCommand,
    DescribeForecastCommand,
    DescribeForecastExportJobCommand,
    DescribePredictorCommand,
    Forecast,
    ForecastClient,
    Schema,
} from "@aws-sdk/client-forecast";
import { S3Client, S3, PutObjectCommand, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { setTimeout } from "timers/promises";
import fs from "fs/promises";

const NAME_SUFFIX = Math.ceil(Date.now() / 1000);

const VALUES = [
    "sameday_1h_available_slots_number",
    "sameday_1h_theoretical_slots_number",
    "sameday_3h_available_slots_number",
    "sameday_3h_theoretical_slots_number",
    "sameday_4h_available_slots_number",
    "sameday_4h_theoretical_slots_number",
    "w2h_available_slots_number",
    "w2h_theoretical_slots_number",
    "nextday_1h_available_slots_number",
    "nextday_1h_theoretical_slots_number",
    "nextday_3h_available_slots_number",
    "nextday_3h_theoretical_slots_number",
    "nextday_4h_available_slots_number",
    "nextday_4h_theoretical_slots_number",
];

const BUCKET = process.env.FORECAST_BUCKET;
const ROLE = process.env.FORECAST_ROLE;

let s3Client = new S3Client({});
let forecastClient = new ForecastClient({});

const METRIC_SCHEMA = {
    Attributes: [
        { AttributeName: "timestamp", AttributeType: "timestamp" },
        { AttributeName: "metric_name", AttributeType: "string" },
        { AttributeName: "metric_value", AttributeType: "integer" },
    ],
} as Schema;

async function processDays(path: string) {
    let out = [];

    let content = await fs.readFile(path, "utf-8");
    let lines = content.split("\n");
    let contents = {} as { [key: string]: string };
    for (let line of lines) {
        let splitted = line.split(",");
        let country = splitted[0];
        let area = splitted[1];
        let day = splitted[2];
        for (let i = 0; i < VALUES.length; i++) {
            let v = VALUES[i];
            let value = splitted[i + 3];
            let outLine = day + "," + area + "," + (value == "0" ? "" : value) + "\n";
            contents[v] = contents[v] || "";
            contents[v] += outLine;
        }
    }
    for (let value of VALUES) {
        let uploadedUrl = await upload("days/" + value + ".csv", contents[value]);
        out.push(uploadedUrl);
    }

    return out;
}

async function processHours(path: string) {
    let s3Paths = [];

    let content = await fs.readFile(path, "utf-8");
    let lines = content.split("\n");
    let contents = {} as { [key: string]: string };
    for (let line of lines) {
        let splitted = line.split("|");
        let country = splitted[0];
        let area = splitted[1];
        let day = splitted[2];
        let hour = "0" + splitted[3];
        let contents = {} as { [key: string]: string };

        for (let i = 0; i < VALUES.length; i++) {
            let v = VALUES[i];
            let value = splitted[i + 4];
            let outLine = day + " " + hour.slice(hour.length - 2) + ":00:00," + area + "," + (value == "0" ? "" : value) + "\n";
            contents[v] = contents[v] || "";
            contents[v] += outLine;
        }
    }

    for (let value of VALUES) {
        let uploadedUrl = await upload("hours/" + value + ".csv", contents[value]);
        s3Paths.push(uploadedUrl);
    }
    return s3Paths;
}

async function upload(key: string, content: string) {
    let out = "s3://" + BUCKET + "/forecast/input/" + key;
    let output = await s3Client.send(
        new PutObjectCommand({
            Bucket: BUCKET,
            Key: "forecast/input/" + key,
            Body: content,
        })
    );
    return out;
}

async function createDS(filepath: string, freq = "D", tsFormat = "yyyy-MM-dd", schema: Schema, wait = true) {
    let jobArn;
    let dataSetGroupArn;
    let dataSetArn;
    let ds = await forecastClient.send(
        new CreateDatasetCommand({
            DatasetName: "data_set_" + NAME_SUFFIX,
            DatasetType: "TARGET_TIME_SERIES",
            DataFrequency: freq, //"D", // H
            Schema: schema,
            Domain: "METRICS",
        })
    );
    if (!ds.DatasetArn) {
        throw new Error("failed to create ds");
    }
    dataSetArn = ds.DatasetArn;

    let dg = await forecastClient.send(
        new CreateDatasetGroupCommand({
            DatasetGroupName: "data_set_group_" + NAME_SUFFIX,
            Domain: "METRICS",
            DatasetArns: [ds.DatasetArn],
        })
    );
    if (!dg.DatasetGroupArn) {
        throw new Error("failed to create dg");
    }
    dataSetGroupArn = dg.DatasetGroupArn;
    let job = await forecastClient.send(
        new CreateDatasetImportJobCommand({
            DatasetArn: ds.DatasetArn,
            DataSource: {
                S3Config: {
                    Path: filepath,
                    RoleArn: ROLE,
                },
            },
            TimestampFormat: tsFormat, // "yyyy-MM-dd", //yyyy-MM-dd HH:mm:ss
            DatasetImportJobName: "import_job_" + NAME_SUFFIX,
            Format: "CSV",
            ImportMode: "FULL",
        })
    );
    if (!job.DatasetImportJobArn) {
        throw new Error("failed to create import job");
    }
    jobArn = job.DatasetImportJobArn;

    if (wait) {
        let status = "";
        while (status != "ACTIVE") {
            await setTimeout(10000);
            let describe = await forecastClient.send(new DescribeDatasetImportJobCommand({ DatasetImportJobArn: jobArn }));
            console.clear();
            console.log("waiting for data import job: " + jobArn + " to finish.." + (Math.round(Math.random()) == 1 ? "." : ".."));

            status = describe.Status || "";
        }
    }

    return {
        jobArn,
        dataSetGroupArn,
        dataSetArn,
    };
}

async function createAutoPredictor(dataSetGroupArn: string, wait = true) {
    let autoPredictor = await forecastClient.send(
        new CreateAutoPredictorCommand({
            PredictorName: "predictor_" + NAME_SUFFIX,
            DataConfig: {
                DatasetGroupArn: dataSetGroupArn,
                AdditionalDatasets: [
                    {
                        Configuration: {
                            CountryCode: ["IT"],
                        },
                        Name: "holiday",
                    },
                ],
            },
            ForecastFrequency: "D",
            ForecastHorizon: 14,
        })
    );
    if (!autoPredictor.PredictorArn) {
        throw new Error("failed to create predictor");
    }

    let status = "";
    if (wait) {
        while (status != "ACTIVE") {
            await setTimeout(5000);
            let describe = await forecastClient.send(
                new DescribeAutoPredictorCommand({
                    PredictorArn: autoPredictor.PredictorArn,
                })
            );
            console.clear();
            console.log("waiting for predictor: " + autoPredictor.PredictorArn + " to finish creating.." + (Math.round(Math.random()) == 1 ? "." : ".."));

            status = describe.Status || "";
        }
    }
    // console.log(autoPredictor);
    return autoPredictor.PredictorArn;
}

async function createForecast(parn: string, wait = true) {
    let out = await forecastClient.send(
        new CreateForecastCommand({
            ForecastName: "forecast_" + NAME_SUFFIX,
            PredictorArn: parn,

            // ForecastTypes: [""],
        })
    );
    if (!out.ForecastArn) {
        throw new Error("failed to create forecast");
    }

    if (wait) {
        let status = "";
        while (status != "ACTIVE") {
            await setTimeout(5000);
            let describe = await forecastClient.send(
                new DescribeForecastCommand({
                    ForecastArn: out.ForecastArn,
                })
            );
            console.clear();
            console.log("waiting for forecast: " + out.ForecastArn + " to finish creating" + (Math.round(Math.random()) == 1 ? "." : ".."));
            status = describe.Status || "";
        }
    }
    return out.ForecastArn;
}

async function exportForecastData(forecastArn: string) {
    let prefix = "forecast/exports/";
    let path = "s3://" + BUCKET + "/" + prefix;
    let job = await forecastClient.send(
        new CreateForecastExportJobCommand({
            ForecastArn: forecastArn,
            Destination: {
                S3Config: { Path: path + NAME_SUFFIX, RoleArn: ROLE },
            },
            ForecastExportJobName: "forecast_export_" + NAME_SUFFIX,
        })
    );
    if (!job.ForecastExportJobArn) {
        throw new Error("failed to create job");
    }

    let status = "";
    while (status != "ACTIVE") {
        await setTimeout(5000);
        let describe = await forecastClient.send(
            new DescribeForecastExportJobCommand({
                ForecastExportJobArn: job.ForecastExportJobArn,
            })
        );
        console.clear();
        console.log("waiting for forecast export: " + job.ForecastExportJobArn + " to finish" + (Math.round(Math.random()) == 1 ? "." : ".."));
        status = describe.Status || "";
    }

    let objects = await s3Client.send(
        new ListObjectsV2Command({
            Bucket: BUCKET,
            Prefix: prefix + NAME_SUFFIX + "/forecast_export_",
        })
    );
    let files = objects.Contents || [];
    let exported = "";
    let header = "metric_name,date,p10,p50,p90\n"; //DEFAULT

    for (let file of files) {
        let output = await s3Client.send(
            new GetObjectCommand({
                Bucket: BUCKET,
                Key: file.Key,
            })
        );
        let body = await output.Body?.transformToString();
        body = body?.split("\n").slice(1).join("\n");
        exported = body + exported;
    }
    await fs.writeFile("out/export_" + NAME_SUFFIX + ".csv", header + exported);
}

async function fullyProcessDays() {
    // download a db export first
    let paths = await processDays("data/dm_delivery_slot_availability.csv");
    // for(let path of paths){
    let path = paths[0];
    let { jobArn, dataSetArn, dataSetGroupArn } = await createDS(path, "D", "yyyy-MM-dd", METRIC_SCHEMA);
    let predictorArn = await createAutoPredictor(dataSetGroupArn);
    let farn = await createForecast(predictorArn);
    await exportForecastData(farn);
    // }
}

async function fullyProcessHours() {
    let paths = await processHours("data/dm_delivery_slot_availability_hours.csv");
    let path = paths[0];
    let { jobArn, dataSetArn, dataSetGroupArn } = await createDS(path, "H", "yyyy-MM-dd HH:mm:ss", METRIC_SCHEMA);
    let predictorArn = await createAutoPredictor(dataSetGroupArn);
    let farn = await createForecast(predictorArn);
    await exportForecastData(farn);
}

// fs.readdir("./out/export2").then(async (data) => {
//     for (let dir of data) {
//         let contents = await fs.readFile("out/export2/" + dir, "utf-8");
//         fs.appendFile("out/joined.csv", contents);
//     }
// });


