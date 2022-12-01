#! /bin/bash
for i in $(aws batch list-jobs --job-queue QueueForFeuerstein --job-status running --output text --query jobSummaryList[*].[jobId])
do
  echo "Cancel Job: $i"
  aws batch terminate-job --job-id $i --reason "Cancelling job."
  echo $?
  echo "Job $i canceled"
done
