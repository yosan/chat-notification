var _ = require('lodash');
var AWS = require('aws-sdk');
var sns = new AWS.SNS({
   apiVersion: '2010-03-31',
   region: 'ap-northeast-1'
});

AWS.config.update({
  region: "ap-northeast-1"
});

function getTextsFromEvent(event) {
  var messages = []
      ;

  messages = _.map(event.Records, function(record) {
    var text = _.get(record, 'dynamodb.NewImage.Text.S'),
        roomId = _.get(record, 'dynamodb.NewImage.RoomId.S'),
        params = null,
        query = null
        ;
    return {
      'Text': text,
      'RoomId': roomId
    }
  })

  return messages;
}

function getUserIdsInRoom(roomId, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient(),
      userIds = null,
      params = null
      ;

  params = {
      TableName : "AWSChatRooms",
      KeyConditionExpression: "#r = :id",
      ExpressionAttributeNames:{
          "#r": "RoomId"
      },
      ExpressionAttributeValues: {
          ":id": roomId
      }
  };

  docClient.query(params, function(err, data) {
    callback(err, data)
  });
}

function getUserEndpoint(userId, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient()
      ;

  var params = {
      TableName: 'AWSChatUsers',
      Key: {
        'UserId': userId
      }
  };

  docClient.get(params, function(err, data) {
    callback(err, data)
  });
}

exports.handle = function(e, ctx) {
  var messages = []
      ;

  // Receive messages
  messages = getTextsFromEvent(e);

  // Get users in the same chat room
  _.forEach(messages, function(message) {
    getUserIdsInRoom(message.RoomId, function(err, data) {
      var userIds = []
        ;

      if (err) {
        console.log(err);
      }

      // Send notification to users
      userIds = _.map(data.Items, function(item) { return item.UserId; })

      userIds.forEach(function(userId) {

        getUserEndpoint(userId, function(err, user) {
          if (err) {
            console.log(err);
          }

          var data = {
            Message: message.Text,
            TargetArn: user.Item.EndpointArn
          }
          sns.publish(data, function(error, responseData){
            if (error) {
              ctx.fail(error);
            } else {
              ctx.succeed(responseData);
            }
          });
        });
      });
    });
  });
}
