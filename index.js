const express = require("express");
const cors = require("cors");
require("dotenv").config();
const ObjectID = require("mongodb").ObjectID;
const bodyParser = require("body-parser");

const app = express();
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false, limit: "50mb" }));
app.use(bodyParser.json());

const port = 5002;

const MongoClient = require("mongodb").MongoClient;
const uri = "mongodb://127.0.0.1:27017/aktcl_report";
const client = new MongoClient(uri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});
client.connect((err) => {
  const reportsCollection = client.db("aktcl_report").collection("reports");
  console.log("user Connection");
  app.get("/leadsReports", (req, res) => {
    reportsCollection.find({}).toArray((err, results) => {
      results.forEach((element) => {
        console.log(element);
      });
    });
  });
  app.get("/updateConnectCall", (req, res) => {
    const updateC = req.body;
    console.log(updateC);
    reportsCollection
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
  app.patch("/update1", async (req, res) => {
    const condition1 = req.body;
    let buldOperation = [];
    let counter = 0;
    console.log(condition1);

    try {
      condition1.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element._id) },
            update: {
              $set: {
                viewStatus: element.viewStatus,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await reportsCollection.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await reportsCollection.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: `users have been updated successfully`,
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
