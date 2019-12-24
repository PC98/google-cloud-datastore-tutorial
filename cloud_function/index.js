let colors = require("colors");
let util = require("util");
let Datastore = require("@google-cloud/datastore");

const datastore = Datastore();

function _createParticleEventObjectForStorage(event, log) {
  let obj = {
    gc_pub_sub_id: event.id,
    device_id: event.attributes.device_id,
    event: event.attributes.event,
    data: event.data,
    published_at: event.attributes.published_at
  };

  if (log) {
    return colors.grey(util.inspect(obj));
  } else {
    return obj;
  }
}

function storeEvent(event) {
  let key = datastore.key("ParticleEvent");

  datastore
    .save({
      key: key,
      data: _createParticleEventObjectForStorage(event)
    })
    .then(() => {
      console.log(
        colors.green("Particle event stored in Datastore!\r\n"),
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
