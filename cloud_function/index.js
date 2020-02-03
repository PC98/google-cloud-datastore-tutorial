let colors = require("colors");
let util = require("util");
let Firestore = require("@google-cloud/firestore");
let Timestamp = require("firebase-admin").firestore.Timestamp;

const datastore = new Firestore();

function _createParticleEventObjectForStorage({ id, attributes, data }) {
  let decodedData = Buffer.from(data, "base64").toString();
  try {
    decodedData = JSON.parse(decodedData);
  } catch (_) {
  } finally {
    return {
      gc_pub_sub_id: id,
      device_id: attributes.device_id,
      event: attributes.event,
      data: decodedData,
      published_at: Timestamp.fromDate(new Date(attributes.published_at))
    };
  }
}

function storeEvent(event) {
  const dbRef = datastore.collection("ParticleEvent");

  const obj = _createParticleEventObjectForStorage(event);

  dbRef
    .add(obj)
    .then(() => {
      console.log(
        colors.green("Particle event stored in Firestore!\r\n"),
        colors.grey(util.inspect(obj))
      );
    })
    .catch(err => {
      console.log(colors.red("There was an error storing the event:"), err);
    });
}

exports.storeToDatastore = (event, context) => {
  console.log(event);
  console.log(context);

  storeEvent({ ...event, id: context.eventId });
};
