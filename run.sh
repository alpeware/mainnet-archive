#!/bin/bash

DATA="data"
LOCK="${DATA}/lock"

[ -f "${LOCK}" ] && { echo "already running" ; exit 0 ;}

mkdir -p "${DATA}" && touch "${LOCK}"

git config user.name github-actions
git config user.email github-actions@github.com

git add .
git commit -m "locked" -a
git push

yarn install --frozen-lockfile
yarn start

rm "${LOCK}"

git add . &> /dev/null
git commit -m "indexed" -a --quiet
git pull --rebase --quiet
git push --quiet
