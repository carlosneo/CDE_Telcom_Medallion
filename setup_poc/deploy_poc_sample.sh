#!/bin/sh

docker_user=$1
cde_user=$2
max_participants=$3
cdp_data_lake_storage=$4

cde_user_formatted=${cde_user//[-._]/}
d=$(date)
fmt="%-30s %s\n"

echo "##########################################################"
printf "${fmt}" "CDE HOL deployment launched."
printf "${fmt}" "launch time:" "${d}"
printf "${fmt}" "performed by CDP User:" "${cde_user}"
printf "${fmt}" "performed by Docker User:" "${docker_user}"
echo "##########################################################"

echo "CREATE DOCKER RUNTIME RESOURCE"
cde job delete --name poc-setup-$cde_user
cde credential delete --name docker-creds-$cde_user
cde credential create --name docker-creds-$cde_user --type docker-basic --docker-server hub.docker.com --docker-username $docker_user
cde resource create --name poc-spark-runtime-$cde_user --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality --image-engine spark3 --type custom-runtime-image
echo "CREATE FILE RESOURCE"
cde resource delete --name poc-setup-$cde_user
cde resource create --name poc-setup-$cde_user --type files
cde resource upload --name poc-setup-$cde_user --local-path sample/utils.py --local-path sample/setup.py

echo "RUN DATAGEN JOB"
echo "RUN POC SETUP JOB"
cde job create --name poc-setup-$cde_user \
  --type spark \
  --mount-1-resource poc-setup-$cde_user \
  --application-file setup.py \
  --runtime-image-resource-name poc-spark-runtime-$cde_user \
  --arg $max_participants \
  --arg $cdp_data_lake_storage \
  --executor-cores 5 \
  --executor-memory "8g"

cde job run \
  --name poc-setup-$cde_user

function loading_icon_job() {
  local loading_animation=( '—' "\\" '|' '/' )

  echo "${1} "

  tput civis
  trap "tput cnorm" EXIT

  while true; do
    job_status=$(cde run list --filter "job[like]%poc-setup-"$cde_user | jq -r "[last] | .[].status")
    if [[ $job_status == "succeeded" ]]; then
      echo "Setup Job Execution Completed"
      break
    else
      for frame in "${loading_animation[@]}" ; do
        printf "%s\b" "${frame}"
        sleep 1
      done
    fi
  done
  printf " \b\n"
}

loading_icon_job "POC Setup Job in Progress"

e=$(date)

echo "##########################################################"
printf "${fmt}" "CDE POC Setup Dategen completed."
printf "${fmt}" "completion time:" "${e}"
printf "${fmt}" "please visit CDE Job Runs UI to view job run"
echo "##########################################################"
