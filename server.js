/*
 *   Tittle:   World Wide Cluster Final Project.
 *   Authors:  Victor Santos, Servio Palacio, Agustin Bazzani.
 *   Source:   server.js
 *   Language: NodeJS
 *   Course:   Final Project for Parallel Computing and Networks, Purdue University.
 */

/* Required Modules */
var http = require('http');
var path = require('path');
var async = require('async');
var socketio = require('socket.io');
var express = require('express');
var geoip = require('geoip-lite');

/* Global Variables */
var router = express();
var server = http.createServer(router);
var io = socketio.listen(server);
var intMinimumPipeCountToDeployTopology = 2;

/* GLobal Arrays, one for Sandboxes and the other for clients  */
var sandboxSockets = [];
var clientSockets = [];
var runningJobs = [];

/* Adding hash array property to the arrays.
 * The reason for this is because we will have
 * an array in each array that stores the keys for
 * each socket. In that way we don't need to iterate
 * the arrays looking for the sockets, its like a hash table
 * for O(1) complexity search. */
sandboxSockets["hash"] = {};
clientSockets["hash"] = {};
runningJobs["hash"] = {};

/* Express Server Path Setup */
router.use(express.static(path.resolve(__dirname, 'client')));

/******************************* PROGRAM BEGINS HERE **************************/

/* When new socket connects, fire provided function */
io.on('connection', function(socket) {

  /* Socket Setup Function */
  socketConnectedSetup(socket);

  console.log(socket.id);
  
  socket.sysInfo.RTTBegin = new Date().getTime();
  socket.emit("sampleRTT");
  
  /* All events will be declared here, and then call the corresponding
   * methods. This way we keep the code clean and understandable. */

  /*********** Core events First **************/
  socket.on('disconnect', function() {
    disconnect(socket);
  });

  /*********** Custom Events Next ***************/
  socket.on('sandboxSendScheduleData', function(packet) {
    sandboxSendScheduleData(socket, packet);
  })

  socket.on("requestSandBoxListing", function() {
    requestSandBoxListing(socket);
  });

  socket.on('jobDeploymentRequest', function(job) {
    jobDeploymentRequest(socket, job);
  })

  socket.on('jobDeploymentResponse', function(results) {
    jobDeploymentResponse(socket, results);
  })

  socket.on('jobDeploymentErrorResponse', function(error) {
    jobDeploymentErrorResponse(socket, error);
  })

  socket.on('requestSocketID', function() {
    socket.emit("socketIDResponse", socket.id);
  })

  socket.on('clusterStatusRequest', function() {
    clusterStatusRequest(socket);
  })
  
  socket.on('sampleRTTResponse', function() {
    console.log('sampleRTTResponse');
    socket.sysInfo.RTTEnd = new Date().getTime();
    socket.sysInfo.RTT = socket.sysInfo.RTTEnd - socket.sysInfo.RTTBegin;
    broadcastRTT(socket);
  });

});

/******************** NEW SOCKET CONNECTED EVENT ******************************/
function socketConnectedSetup(socket) {

  /* Save this socket as pipe or as client */
  if (socket.handshake.query.isClient === 'true') {
    socket.isClient = true;
    socket.sysInfo = JSON.parse(socket.handshake.query.sysInfo);
    /* Extracting geo data if not behind NAT*/
    socket.sysInfo.geo = geoip.lookup(socket.sysInfo.publicIP);
    clientSockets.push(socket);
    clientSockets.hash[socket.id] = socket;
    console.log("Client connected");

  }
  else {
    socket.isClient = false;
    socket.sysInfo = JSON.parse(socket.handshake.query.sysInfo);
    /* Extracting geo data if not behind NAT*/
    socket.sysInfo.geo = geoip.lookup(socket.sysInfo.publicIP);
    console.log(socket.sysInfo.publicIP);
    sandboxSockets.push(socket);
    sandboxSockets.hash[socket.id] = socket;
    broadcastPipeConnected(socket);
    
    console.log("SandBox connected");

    if (sandboxSockets.length >= intMinimumPipeCountToDeployTopology) {

      var pipes = [];
      console.log("Deploying topology");

      sandboxSockets.forEach(function(p) {
        var pipeInfo = {};
        pipeInfo.id = p.id;
        pipeInfo.sysInfo = p.sysInfo;
        pipes.push(pipeInfo)
      });
      /* Randomizing Server in Topology */
      var intServer = new Date().getTime() % sandboxSockets.length;

      socket.emit("sandboxDeployTopology", JSON.stringify(pipes));

    }

    socket.emit("sandboxRTT");
  }

}


/******************** SOCKET EVENTS ************************************/
function clusterStatusRequest(socket) {
  var status = {};
  status.ClientCount = clientSockets.length;
  status.SandBoxCount = sandboxSockets.length;
  status.runningJobCount = runningJobs.length;

  socket.emit("clusterStatusResponse", status);
}

function jobDeploymentResponse(socket, results) {

  console.log("Response Arrived:" + socket.id);
  /* The results Object contains two properties
   * {clientSocketId, result}*/
  var clientSocket = clientSockets.hash[results.clientSocketId];
  /* Indicating the identity of the sandbox */
  results.sandboxSocketId = socket.id;
  /* Extracting the sandbox socket and emit*/
  if (clientSocket)
    clientSocket.emit("jobExecutionResponse", results);

}


function jobDeploymentRequest(socket, job) {

  /* The Job Object contains two properties
   * {clientSocketId, sdbxs}*/
    console.log("Request received:" + socket.id);
  /*Setup Job */
  runningJobs.push(job);
  runningJobs.hash[job.clientSocketId] = job;

  /* Execute job sandboxes broadcast in parallel, each sandbox have three
   * properties, {clientSocketId, sandboxSocketId, jobCode }, where jobCode
   * is an Object containing the code to be executed */
  /* Execute in parallel with async */
  async.each(job.sdbxs, function(sdboxJob) {
    
      console.log("Sending request:");

    /* Extracting the sandbox socket and emit*/
    var sdbxSocket = sandboxSockets.hash[sdboxJob.sandboxSocketId];

    /* If the sandbox is still connected (This is asynchronous) */
    if (sdbxSocket)
      sdbxSocket.emit("jobExecutionRequest", sdboxJob)
  });
}

function jobDeploymentErrorResponse(socket, error) {
  
  /* The data Object contains two properties
   * {clientSocketId, error}*/
  var clientSocket = clientSockets.hash[error.clientSocketId];
  /* Indicating the identity of the failed sandbox */
  error.sandboxSocketId = socket.id;
  /* Extracting the sandbox socket and emit*/
  if (clientSocket)
    clientSocket.emit("jobExecutionErrorResponse", error);

}



function sandboxSendScheduleData(socket, packet) {

  var localPacket = JSON.parse(packet);
  console.log("Receiving Scheduled Data " + localPacket.payload);
  socket.emit('sandboxReceiveScheduleData', JSON.stringify(localPacket));
}


/* Tell this server that this socket is a client and not a pipe */
function requestSandBoxListing(socket) {

  var pipes = [];

  /* Retrieving all pipe sockets and storing into a list */
  sandboxSockets.forEach(function(p) {
    var pipeInfo = {};
    pipeInfo.id = p.id;
    pipeInfo.sysInfo = p.sysInfo;
    pipes.push(pipeInfo)
  });

  socket.emit("reponseSandboxListing", pipes);

}

/* On disconnected, remove socket and notify clients that pipe is offline */
function disconnect(socket) {

  if (socket.isClient) {
    /* A Client have been disconnected, remove it */
    clientSockets.splice(clientSockets.indexOf(socket), 1);
    delete clientSockets.hash[socket.id];
    console.log("Removed client socket");

  }
  else {
    /* A pipe have been disconnected, remove and broadcast new status */
    sandboxSockets.splice(sandboxSockets.indexOf(socket), 1);
    broadcastPipeDisconnected(socket);
    delete sandboxSockets.hash[socket.id];
    console.log("Removed SandBox socket");
  }
}

/*************************** HELPER FUNCTIONS **********************************/


/* Broadcast when a pipe Connects */
/* TODO: Change this to async */
function broadcastPipeConnected(pSock) {

  var pipeInfo = {};
  pipeInfo.id = pSock.id;
  pipeInfo.sysInfo = pSock.sysInfo;

  clientSockets.forEach(function(socket) {
    socket.emit('sandBoxConnected', pipeInfo);
  });
}

/* Broadcast when a pipe Disconnects */
/* TODO: Change this to async */
function broadcastPipeDisconnected(pSock) {
  var pipeInfo = {};
  pipeInfo.id = pSock.id;

  clientSockets.forEach(function(socket) {
    socket.emit('sandboxDisconnected', pipeInfo);
  });
}

/* Broadcast RTT */
function broadcastRTT(pSock) {
  var pipeInfo = {};
  pipeInfo.id = pSock.id;
  pipeInfo.RTT = pSock.sysInfo.RTT;
  
  clientSockets.forEach(function(socket) {
    console.log(pipeInfo.RTT);
    socket.emit('RTTRefresh', pipeInfo);
  });
  
}


/*************************** SERVER CONNECTION SETTING  ************************/
server.listen(process.env.PORT || 3000, process.env.IP || "0.0.0.0", function() {
  var addr = server.address();
  console.log("World Wide Cluster server listening at", addr.address + ":" + addr.port);
});



