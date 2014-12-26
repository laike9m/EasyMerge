var JOBLINK = "http://10.100.1.35:50030/jobdetails.jsp?jobid=";
var TARGET = 'Running job: ';

function insertLink(textToAdd) {
  var i = textToAdd.indexOf(TARGET);
  if (i != -1) {
    var jobId = textToAdd.slice(i + TARGET.length);
    textToAdd = textToAdd.slice(0, i + TARGET.length)
      + "<a style='color:red' href='" + JOBLINK + jobId + "'>" + jobId + "</a>";
  }
  return textToAdd;
}
