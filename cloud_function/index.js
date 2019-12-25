let colors = require("colors");
let util = require("util");
let Firestore = require("@google-cloud/firestore");
let Timestamp = require("firebase-admin").firestore.Timestamp;

const datastore = new Firestore();

function _createParticleEventObjectForStorage({ id, attributes, data }, log) {
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

function storeEvent(event) {
  const dbRef = datastore.collection("ParticleEvent");

  dbRef
    .add(_createParticleEventObjectForStorage(event))
    .then(() => {
      console.log(
        colors.green("Particle event stored in Firestore!\r\n"),
        _createParticleEventObjectForStorage(event, true)
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
