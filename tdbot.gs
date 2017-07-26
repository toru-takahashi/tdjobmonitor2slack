/**
 * Monitor TreasureData Jobs, and notify it to Slack
 *
 */
function monitorTdJobs(endpoint, apikey, duration_threshold, queueing_threshold, webhook, channel) {  
  var jobs = getRunningQueuingJobs_(endpoint, apikey);
  Logger.log(jobs);
  
  var runnings = [];
  var queuings = [];
  
  // Split Running and Queued
  for (var i = 0; i < jobs.length; i++) {
    if (jobs[i]['status'] == 'running') {
      runnings.push(jobs[i]);
    } else if (jobs[i]['status'] == 'queued') {
      queuings.push(jobs[i]);
    }
  }

  var date = new Date();
  var offset = date.getTimezoneOffset() * 60 * 1000;
  var now = Math.floor( (date.getTime() + offset) / 1000)
  var duration = 0;
  
  var message = '';
  
  // Done if the number of queued jobs are under the threshold.
  if (queuings.length <= queueing_threshold) {
    return false;
  }
  
  message += 'Hello! *'+ queuings.length +' Jobs* are queued at _' + date + '_.\n'
  message += 'The Long Running Jobs over ' + duration_threshold + ' mins are below.\n'
  
  attachments = [];
  
  // Pickup a Running Job if it exceeded the threshold
  for (var i = 0; i < runnings.length; i++) {
      duration = now - (Date.parse(runnings[i]['created_at'].replace(/-/g, '/').replace(' UTC', '')))/1000;
      
      if (Math.floor(duration/60) >= duration_threshold) {
        var text = 'User: ' + runnings[i]['user_name'] + '\n';
        text += 'Status: '  + runnings[i]['status'] + '\n';
        text += 'type: '  + runnings[i]['type'] + '\n';
        text += 'Duration: '  + Math.floor(duration/60) + 'mins\n';
        
        var color = "#00ff00";
        
        if (Math.floor(duration/60) >= duration_threshold * 2) {
          color = "#ff0000";
        }
        
        attachments.push(
          {
            color: color,
            title: 'JOB ' + runnings[i]['job_id'],
            title_link: 'https://' + endpoint + '/app/jobs/'+runnings[i]['job_id'],
            text: text
          }
        );
      }
  }
  
  Logger.log(attachments);
  postSlack_(webhook, channel, message, attachments);
}

// Get Running and Queuing Jobs
function getRunningQueuingJobs_(endpoint, apikey) {
  var options = {
    "method": "GET",
    "contentType" : "application/json",
    "headers" : {
      "Authorization" : "TD1 " + apikey
    }
  };
  // status=running contains Running and Queued
  var response = UrlFetchApp.fetch('https://' + endpoint + '/v3/job/list?status=running', options); 
  var response_json = JSON.parse(response.getContentText());
  return response_json['jobs']
}


function postSlack_(webhook, channel, message, attachments) {
  var jsonData =
  {
     "channel" : channel,
     "text" : message,
     "attachments":  attachments
  };
  var payload = JSON.stringify(jsonData);
  var options =
  {
    "method" : "post",
    "contentType" : "application/json",
    "payload" : payload
  };
 
  UrlFetchApp.fetch(webhook, options);
}