run_mq_test() {
  # start
  echo "$(date) Waiting for 3 minutes to converge"
  sleep 180

  echo "$(date) 80% error rate for queue 1 for 3m"
  curl -sSL "http://127.0.0.1:9003/?index=0&duration=3m&error_rate=0.8" | jq -r .
  sleep 180

  # wait for 60 seconds for the circuit to be half-open
  sleep 60

  echo "$(date) 80% error rate for queue 2 for 3m"
  curl -sSL "http://127.0.0.1:9003/?index=1&duration=3m&error_rate=0.8" | jq -r .
  sleep 180

  # wait for 60 seconds for the circuit to be half-open
  sleep 60

  echo "$(date) 80% error rate for queue 1 and 2 for 5m and 7m"
  curl -sSL "http://127.0.0.1:9003/?index=0&duration=5m&error_rate=0.8" | jq -r .
  curl -sSL "http://127.0.0.1:9003/?index=1&duration=7m&error_rate=0.8" | jq -r .
  sleep 120
}

run_mq_test
