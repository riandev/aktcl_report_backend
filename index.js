const express = require("express");
const cors = require("cors");
const _ = require("lodash");
require("dotenv").config();
const ObjectID = require("mongodb").ObjectID;
const bodyParser = require("body-parser");

const app = express();
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false, limit: "100mb" }));
app.use(bodyParser.json());

const port = 5002;

const MongoClient = require("mongodb").MongoClient;
const { sum } = require("lodash");
const uri = "mongodb://127.0.0.1:27017/aktcl_report";
const client = new MongoClient(uri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});
client.connect((err) => {
  //   const reportsCollection = client.db("aktcl_report").collection("reports");
  const finalReport = client.db("aktcl_report").collection("final_report");
  const territoryReport = client
    .db("aktcl_report")
    .collection("territory_Report");
  const questionResult = client
    .db("aktcl_report")
    .collection("question_result");
  console.log("user Connection");

  app.get("/updateConnectCall", (req, res) => {
    const updateC = req.body;
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q1: "Yes" }, { q1: "No" }],
          },
        },
      ])
      .toArray((err, results) => {
        console.log(results);
        res.send(results);
      });
  });
  app.get("/getTrueContact", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [
              { q1: "Yes" },
              { q2: "Yes" },
              { q6: "Yes" },
              { q7: "Marise" },
            ],
          },
        },
      ])
      .toArray((err, trueContact) => {
        res.send(trueContact);
      });
  });
  app.get("/nonSOB1", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [
              {
                $or: [{ q3: "Others" }],
              },
              {
                $or: [
                  { q4: "13 days" },
                  { q4: "14 days" },
                  { q4: "15 days" },
                  { q4: "3 weeks" },
                  { q4: "1 month" },
                  { q4: "2 months" },
                  { q4: "3 months" },
                  { q4: "4 months" },
                  { q4: "5 months" },
                  { q4: "6 months" },
                  { q4: "1 year" },
                  { q4: "More then 1 year" },
                ],
              },
            ],
          },
        },
      ])
      .toArray((err, nonSOB1) => {
        res.send(nonSOB1);
      });
  });
  app.get("/nonSOB2", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [
              {
                $or: [{ q3: "Others" }, { q3: "Marise" }],
              },
              {
                $or: [
                  { q4: "1 day" },
                  { q4: "2 days" },
                  { q4: "3 days" },
                  { q4: "4 days" },
                  { q4: "5 days" },
                  { q4: "6 days" },
                  { q4: "7 days" },
                  { q4: "8 days" },
                  { q4: "9 days" },
                  { q4: "10 days" },
                  { q4: "11 days" },
                  { q4: "12 days" },
                ],
              },
              {
                $or: [{ q5: "Others" }],
              },
            ],
          },
        },
      ])
      .toArray((err, nonSOB2) => {
        res.send(nonSOB2);
      });
  });
  app.get("/extMSB", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [
              {
                $or: [{ q3: "Marise" }],
              },
              {
                $or: [
                  { q4: "13 days" },
                  { q4: "14 days" },
                  { q4: "15 days" },
                  { q4: "3 weeks" },
                  { q4: "1 month" },
                  { q4: "2 months" },
                  { q4: "3 months" },
                  { q4: "4 months" },
                  { q4: "5 months" },
                  { q4: "6 months" },
                  { q4: "1 year" },
                  { q4: "More then 1 year" },
                ],
              },
            ],
          },
        },
      ])
      .toArray((err, extMSB) => {
        res.send(extMSB);
      });
  });
  app.get("/notContacted1", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q1: "No" }],
          },
        },
      ])
      .toArray((err, notContacted1) => {
        res.send(notContacted1);
      });
  });
  app.get("/notContacted2", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q2: "No" }],
          },
        },
      ])
      .toArray((err, notContacted2) => {
        res.send(notContacted2);
      });
  });
  app.get("/notContacted3", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q6: "No" }],
          },
        },
      ])
      .toArray((err, notContacted3) => {
        res.send(notContacted3);
      });
  });
  app.get("/notContacted4", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q7: "Others" }],
          },
        },
      ])
      .toArray((err, notContacted4) => {
        res.send(notContacted4);
      });
  });
  app.get("/finalNotContacted", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [
              { notContacteda: 1 },
              { notContactedb: 1 },
              { notContactedc: 1 },
              { notContactedd: 1 },
            ],
          },
        },
      ])
      .toArray((err, finalNotContacted) => {
        res.send(finalNotContacted);
      });
  });
  app.get("/finalNotContacted2", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [
              {
                $or: [{ notContacted: 1 }],
              },
              {
                $or: [{ nonSOB1: 1 }, { nonSOB2_Final: 1 }, { extMSB: 1 }],
              },
            ],
          },
        },
      ])
      .toArray((err, finalNotContacted2) => {
        res.send(finalNotContacted2);
      });
  });
  //Final Not Contacted
  app.get("/pureNotContacted", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ notContacted: 1 }],
          },
        },
      ])
      .toArray((err, pureNotContacted) => {
        res.send(pureNotContacted);
      });
  });
  // Final True Contact
  app.get("/finalTrueContact", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ trueContact: 1 }],
          },
        },
      ])
      .toArray((err, finalTrueContact) => {
        res.send(finalTrueContact);
      });
  });
  app.get("/falseContact", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [
              { notContacted: 1 },
              { nonSOB1: 1 },
              { nonSOB2_Final: 1 },
              { extMSB: 1 },
            ],
          },
        },
      ])
      .toArray((err, falseContact) => {
        res.send(falseContact);
      });
  });
  app.get("/finalFalseContact", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ falseContactFinal: 1 }],
          },
        },
      ])
      .toArray((err, falseContact) => {
        res.send(falseContact);
      });
  });
  app.get("/verifyFalseContact", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [{ trueContact: 1 }, { falseContactFinal: 1 }],
          },
        },
      ])
      .toArray((err, verifyFalseContact) => {
        res.send(verifyFalseContact);
      });
  });
  app.get("/noFreeSample", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q9: "0" }],
          },
        },
      ])
      .toArray((err, noFreeSample) => {
        res.send(noFreeSample);
      });
  });
  app.get("/lessFreeSample", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q9: "1" }, { q9: "2" }],
          },
        },
      ])
      .toArray((err, lessFreeSample) => {
        res.send(lessFreeSample);
      });
  });
  app.get("/teaSnaks", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $or: [{ q11: "No" }],
          },
        },
      ])
      .toArray((err, teaSnaks) => {
        res.send(teaSnaks);
      });
  });
  app.get("/retention", (req, res) => {
    finalReport
      .aggregate([
        {
          $match: {
            $and: [
              {
                $or: [{ q3: "Marise" }],
              },
              {
                $or: [
                  { q4: "1 day" },
                  { q4: "2 days" },
                  { q4: "3 days" },
                  { q4: "4 days" },
                  { q4: "5 days" },
                  { q4: "6 days" },
                  { q4: "7 days" },
                  { q4: "8 days" },
                  { q4: "9 days" },
                  { q4: "10 days" },
                  { q4: "11 days" },
                  { q4: "12 days" },
                ],
              },
              {
                $or: [
                  { q5: "Darby" },
                  { q5: "Hollywood" },
                  { q5: "Pilot" },
                  { q5: "Royels" },
                  { q5: "Sheikh" },
                ],
              },
            ],
          },
        },
      ])
      .toArray((err, retention) => {
        res.send(retention);
      });
  });
  app.get("/finalReport", (req, res) => {
    finalReport.find({}).toArray((err, finalReport) => {
      res.send(finalReport);
    });
  });
  app.get("/analyze_import", async (req, res) => {
    async function analyzeData() {
      let result = [];
      let valid_total = 0;
      let connected_total = 0;
      let true_total = 0;
      let notConnected_total = 0;
      let noSOB1_total = 0;
      let nonSOB2_total = 0;
      let extMSB_total = 0;
      let falseContact_total = 0;
      let noFreeSample_total = 0;
      let lessFreeSample_total = 0;
      let teaSnaks_total = 0;
      let retention_total = 0;
      try {
        let data = await finalReport.find({}).toArray();
        let users = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.TM_USER_NAME;
        });
        for (user in users) {
          result.push({
            userId: user,
            userName: users[user][0].TM_NAME,
            teritory: users[user][0].TERITORY_NAME,
            valid_Data_count: users[user].filter(
              (x) => x.data_status === "Valid_Data"
            ).length,
            connected_Call_count: users[user].filter(
              (x) => x.connectedCall === 1
            ).length,
            true_Contact_count: users[user].filter((x) => x.trueContact === 1)
              .length,
            not_Contacted_count: users[user].filter((x) => x.notContacted === 1)
              .length,
            non_SOB1_count: users[user].filter((x) => x.nonSOB1 === 1).length,
            non_SOB2_count: users[user].filter((x) => x.nonSOB2_Final === 1)
              .length,
            ext_MSB_count: users[user].filter((x) => x.extMSB === 1).length,
            false_Contact_count: users[user].filter(
              (x) => x.falseContactFinal === 1
            ).length,
            no_Free_Sample: users[user].filter((x) => x.noFreeSample === 1)
              .length,
            less_Free_Sample: users[user].filter((x) => x.lessFreeSample === 1)
              .length,
            teaSnaks: users[user].filter((x) => x.teaSnaks === 1).length,
            retention: users[user].filter((x) => x.retention === 1).length,
            target: 50,
          });
          valid_total += users[user].filter(
            (x) => x.data_status === "Valid_Data"
          ).length;
          connected_total += users[user].filter(
            (x) => x.connectedCall === 1
          ).length;
          true_total += users[user].filter((x) => x.trueContact === 1).length;
          notConnected_total += users[user].filter(
            (x) => x.notContacted === 1
          ).length;
          noSOB1_total += users[user].filter((x) => x.nonSOB1 === 1).length;
          nonSOB2_total += users[user].filter(
            (x) => x.nonSOB2_Final === 1
          ).length;
          extMSB_total += users[user].filter((x) => x.extMSB === 1).length;
          falseContact_total += users[user].filter(
            (x) => x.falseContactFinal === 1
          ).length;
          noFreeSample_total += users[user].filter(
            (x) => x.noFreeSample === 1
          ).length;
          lessFreeSample_total += users[user].filter(
            (x) => x.lessFreeSample === 1
          ).length;
          teaSnaks_total += users[user].filter((x) => x.teaSnaks === 1).length;
          retention_total += users[user].filter(
            (x) => x.retention === 1
          ).length;
        }
        console.log("Total Data", data.length);
        console.log("Unique Users", result);
        result = result.map((r) => {
          return {
            ...r,
            valid_total,
            connected_total,
            true_total,
            notConnected_total,
            noSOB1_total,
            nonSOB2_total,
            extMSB_total,
            falseContact_total,
            noFreeSample_total,
            lessFreeSample_total,
            teaSnaks_total,
            retention_total,
          };
        });
        insertResult(result);
      } catch (e) {
        console.log(e.message);
      }
    }

    async function insertResult(data) {
      try {
        await questionResult.remove({});
        await questionResult.insertMany(JSON.parse(JSON.stringify(data)));
        console.log("Inserted");
        res.send(`
        <h1>Imported Successfully.</h1>
        <h3> Please Check your DB</h3>
        `);
      } catch (e) {
        res.send("Error", e.message);
      }
    }
    analyzeData();
  });
  //Teritorry Reports
  app.get("/analyze_teritorry", async (req, res) => {
    async function analyzeData() {
      let territoryResult = [];

      let grandConnectedCall = 0;
      let grandTarget = 0;
      let grandValiddata = 0;
      let grandTrueContact = 0;
      let grandNotContacted = 0;
      let grandNonSOB1 = 0;
      let grandNonSOB2 = 0;
      let grandextMSB = 0;
      let grandFalseContact = 0;
      let grandNoFreeSample = 0;
      let grandLessFreeSample = 0;
      let grandTeaSnaks = 0;
      let grandRetention = 0;
      try {
        let data = await questionResult.find({}).toArray();
        let territories = _.groupBy(
          JSON.parse(JSON.stringify(data)),
          function (d) {
            return d.teritory;
          }
        );
        for (territory in territories) {
          territoryResult.push({
            userId: territory,
            sumConnectedCall: territories[territory]
              .filter((x) => x.connected_Call_count)
              .map((x) => Number(x.connected_Call_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumTarget: territories[territory]
              .filter((x) => x.target)
              .map((x) => Number(x.target))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumValiddata: territories[territory]
              .filter((x) => x.valid_Data_count)
              .map((x) => Number(x.valid_Data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumTrueContact: territories[territory]
              .filter((x) => x.true_Contact_count)
              .map((x) => Number(x.true_Contact_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumNotContacted: territories[territory]
              .filter((x) => x.not_Contacted_count)
              .map((x) => Number(x.not_Contacted_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumNonSOB1: territories[territory]
              .filter((x) => x.non_SOB1_count)
              .map((x) => Number(x.non_SOB1_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumNonSOB2: territories[territory]
              .filter((x) => x.non_SOB2_count)
              .map((x) => Number(x.non_SOB2_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumextMSB: territories[territory]
              .filter((x) => x.ext_MSB_count)
              .map((x) => Number(x.ext_MSB_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumFalseContact: territories[territory]
              .filter((x) => x.false_Contact_count)
              .map((x) => Number(x.false_Contact_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumNoFreeSample: territories[territory]
              .filter((x) => x.no_Free_Sample)
              .map((x) => Number(x.no_Free_Sample))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumLessFreeSample: territories[territory]
              .filter((x) => x.less_Free_Sample)
              .map((x) => Number(x.less_Free_Sample))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumTeaSnaks: territories[territory]
              .filter((x) => x.teaSnaks)
              .map((x) => Number(x.teaSnaks))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            sumRetention: territories[territory]
              .filter((x) => x.retention)
              .map((x) => Number(x.retention))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
          });
          grandConnectedCall += territories[territory]
            .filter((x) => x.connected_Call_count)
            .map((x) => Number(x.connected_Call_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandTarget += territories[territory]
            .filter((x) => x.target)
            .map((x) => Number(x.target))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          (grandValiddata += territories[territory]
            .filter((x) => x.valid_Data_count)
            .map((x) => Number(x.valid_Data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0)),
            (grandTrueContact += territories[territory]
              .filter((x) => x.true_Contact_count)
              .map((x) => Number(x.true_Contact_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0));
          grandNotContacted += territories[territory]
            .filter((x) => x.not_Contacted_count)
            .map((x) => Number(x.not_Contacted_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandNonSOB1 += territories[territory]
            .filter((x) => x.non_SOB1_count)
            .map((x) => Number(x.non_SOB1_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandNonSOB2 += territories[territory]
            .filter((x) => x.non_SOB2_count)
            .map((x) => Number(x.non_SOB2_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandextMSB += territories[territory]
            .filter((x) => x.ext_MSB_count)
            .map((x) => Number(x.ext_MSB_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandFalseContact += territories[territory]
            .filter((x) => x.false_Contact_count)
            .map((x) => Number(x.false_Contact_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandNoFreeSample += territories[territory]
            .filter((x) => x.no_Free_Sample)
            .map((x) => Number(x.no_Free_Sample))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandLessFreeSample += territories[territory]
            .filter((x) => x.less_Free_Sample)
            .map((x) => Number(x.less_Free_Sample))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandTeaSnaks += territories[territory]
            .filter((x) => x.teaSnaks)
            .map((x) => Number(x.teaSnaks))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          grandRetention += territories[territory]
            .filter((x) => x.retention)
            .map((x) => Number(x.retention))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
        }
        console.log("Total Data", data.length);
        console.log("Unique Users", territoryResult.length);
        territoryResult = territoryResult.map((r) => {
          return {
            ...r,
            grandConnectedCall,
            grandTarget,
            grandValiddata,
            grandTrueContact,
            grandNotContacted,
            grandNonSOB1,
            grandNonSOB2,
            grandextMSB,
            grandFalseContact,
            grandNoFreeSample,
            grandLessFreeSample,
            grandTeaSnaks,
            grandRetention,
          };
        });
        insertResult(territoryResult);
      } catch (e) {
        console.log(e.message);
      }
    }

    async function insertResult(data) {
      try {
        await territoryReport.remove({});
        await territoryReport.insertMany(JSON.parse(JSON.stringify(data)));
        console.log("Inserted");
        res.send(`
        <h1>Territorry Successfully.</h1>
        <h3> Please Check your DB</h3>
        `);
      } catch (e) {
        res.send("Error", e.message);
      }
    }
    analyzeData();
  });
  app.get("/reportTable", (req, res) => {
    questionResult.find({}).toArray((err, reportTable) => {
      res.send(reportTable);
    });
  });
  app.get("/territoryReports", (req, res) => {
    territoryReport.find({}).toArray((err, territoryReports) => {
      res.send(territoryReports);
    });
  });
  //Connected Call Updated to Database
  app.patch("/update1", async (req, res) => {
    const condition1 = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      condition1.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                connectedCall: element.connectedCall,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: true,
      });
    } catch (error) {
      console.log(error);
    }
  });
  // Update True Contact
  app.patch("/updateTrueContact", async (req, res) => {
    const trueContact = req.body;
    console.log(trueContact);
    let buldOperation = [];
    let counter = 0;

    try {
      trueContact.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                trueContact: element.trueContact,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update non SOB1
  app.patch("/updatenonSOB1", async (req, res) => {
    const nonSOB1 = req.body;
    console.log(nonSOB1);
    let buldOperation = [];
    let counter = 0;

    try {
      nonSOB1.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                nonSOB1: element.nonSOB1,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Non SOB2
  app.patch("/updatenonSOB2", async (req, res) => {
    const nonSOB2 = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      nonSOB2.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                nonSOB2_Final: element.nonSOB2_Final,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.send({
        status: true,
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update EXT MSB
  app.patch("/updateextMSB", async (req, res) => {
    const extMSB = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      extMSB.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                extMSB: element.extMSB,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Not Contacted 1
  app.patch("/updateNotContacted1", async (req, res) => {
    const notContacted1 = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      notContacted1.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                notContacteda: element.notContacteda,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Not Contacted 2
  app.patch("/updateNotContacted2", async (req, res) => {
    const notContacted2 = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      notContacted2.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                notContactedb: element.notContactedb,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Not Contacted 3
  app.patch("/updateNotContacted3", async (req, res) => {
    const notContacted3 = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      notContacted3.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                notContactedc: element.notContactedc,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Not Contacted 4
  app.patch("/updateNotContacted4", async (req, res) => {
    const notContacted4 = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      notContacted4.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                notContactedd: element.notContactedd,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Final Not Contacted
  app.patch("/updateFinalNotContacted", async (req, res) => {
    const finalNotContacted = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      finalNotContacted.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                notContacted: element.notContacted,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Pure Not Contacted
  app.patch("/updateFinalPureNotContacted", async (req, res) => {
    const finalPureNotContacted = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      finalPureNotContacted.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                notContacted: element.notContacted,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update False Contact
  app.patch("/updateFalseContact", async (req, res) => {
    const falseContact = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      falseContact.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                falseContactFinal: element.falseContactFinal,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Verified True Contact
  app.patch("/updateVerifiedTrueContact", async (req, res) => {
    const verifyTrueContact = req.body;
    console.log(verifyTrueContact);
    let buldOperation = [];
    let counter = 0;

    try {
      verifyTrueContact.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                trueContact: element.trueContact,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update No free Sample
  app.patch("/updateNoFreeSample", async (req, res) => {
    const noFreeSample = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      noFreeSample.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                noFreeSample: element.noFreeSample,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Less free Sample
  app.patch("/updateLessFreeSample", async (req, res) => {
    const lessFreeSample = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      lessFreeSample.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                lessFreeSample: element.lessFreeSample,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Tea Snaks
  app.patch("/updateTeaSnaks", async (req, res) => {
    const teaSnaks = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      teaSnaks.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                teaSnaks: element.teaSnaks,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
  //Update Rention
  app.patch("/updateRetention", async (req, res) => {
    const retention = req.body;
    let buldOperation = [];
    let counter = 0;

    try {
      retention.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                retention: element.retention,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await finalReport.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await finalReport.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: "true",
      });
    } catch (error) {
      console.log(error);
    }
  });
});

app.get("/", (req, res) => {
  res.send("Hello World!");
});

app.listen(port);
