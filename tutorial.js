let colors = require("colors");
let util = require("util");
let PubSub = require("@google-cloud/pubsub");
let Firestore = require("@google-cloud/firestore");
let Timestamp = require("firebase-admin").firestore.Timestamp;

/* CONFIGURATION */
let config = {
  gcpProjectId: "praan-pwa-262913",
  gcpPubSubSubscriptionName: "projects/praan-pwa-262913/subscriptions/test_sub",
  gcpServiceAccountKeyFilePath: "./gcp_private_key.json"
};
_checkConfig();
/* END CONFIGURATION */

/* PUBSUB */
console.log(colors.magenta("Authenticating PubSub with Google Cloud..."));
const pubsub = new PubSub({
  projectId: config.gcpProjectId,
  keyFilename: config.gcpServiceAccountKeyFilePath
});
console.log(colors.magenta("Authentication successful!"));

const subscription = pubsub.subscription(config.gcpPubSubSubscriptionName);
subscription.on("message", message => {
  console.log("HERE");
  const date = new Date(message.attributes.published_at);
  if (isNaN(date.getTime())) {
    message.ack();
    return;
  }
  console.log(
    colors.cyan("Particle event received from Pub/Sub!\r\n"),
    _createParticleEventObjectForStorage(message, true)
  );
  // Called every time a message is received.
  // message.id = ID used to acknowledge its receival.
  // message.data = Contents of the message.
  // message.attributes = Attributes of the message.

  storeEvent(message);
});
/* END PUBSUB */

/* FIRESTORE */
console.log(colors.magenta("Authenticating Firestore with Google Cloud..."));
const datastore = new Firestore({
  projectId: config.gcpProjectId,
  keyFilename: config.gcpServiceAccountKeyFilePath
});
console.log(colors.magenta("Authentication successful!"));

function storeEvent(message) {
  let dbRef = datastore.collection("ParticleEvent");

  // If connection is cold, following doc and get calls help:
  dbRef
    .doc()
    .get()
    .then(_ => {
      dbRef.add(_createParticleEventObjectForStorage(message)).then(() => {
        console.log(
          colors.green("Particle event stored in Firestore!\r\n"),
          _createParticleEventObjectForStorage(message, true)
        );
        message.ack();
      });
    })
    .catch(err => {
      console.log(colors.red("There was an error storing the event:"), err);
    });
}
/* END FIRESTORE */

/* HELPERS */
function _checkConfig() {
  if (config.gcpProjectId === "" || !config.gcpProjectId) {
    console.log(
      colors.red(
        "You must set your Google Cloud Platform project ID in tutorial.js"
      )
    );
    process.exit(1);
  }
  if (
    config.gcpPubSubSubscriptionName === "" ||
    !config.gcpPubSubSubscriptionName
  ) {
    console.log(
      colors.red(
        "You must set your Google Cloud Pub/Sub subscription name in tutorial.js"
      )
    );
    process.exit(1);
  }
}

function _createParticleEventObjectForStorage({ id, attributes, data }, log) {
  console.log();
  console.log(attributes.published_at);
  let obj = {
    gc_pub_sub_id: id,
    device_id: attributes.device_id,
    event: attributes.event,
    data: JSON.parse(Buffer.from(data, "base64").toString()),
    published_at: Timestamp.fromDate(new Date(attributes.published_at))
  };

  if (log) {
    return colors.grey(util.inspect(obj));
  } else {
    return obj;
  }
}
/* END HELPERS */
