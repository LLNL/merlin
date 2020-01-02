#!/bin/bash

# Ensure CHANGELOG version and merlin/__init__.py version are equal
CLVER=`grep CHANGELOG.md -m 1 -o -e "\[[0-9]\+\.[0-9]\+\.[0-9]\+\]" | grep -oe "[0-9]\+\.[0-9]\+\.[0-9]\+"`
MVER=`grep merlin/__init__.py -m 1 -o -e "[0-9]\+\.[0-9]\+\.[0-9]\+"`
if [[ $CLVER != $MVER ]]
then
  echo "Error: merlin/__init__.py version (${MVER}) different from CHANGELOG.md verison (${CLVER})"
    exit 1
fi


# Ensure CHANGELOG version and git tag version are equal
TAGVER=`git tag | grep "." | tail -1`
if [[ $TAGVER != $CLVER ]]
then
  echo "Error: CHANGELOG.md verison (${CLVER}) different from most recent git tag (${TAGVER})"
    exit 1
fi


# Ensure CHANGELOG has changed
if ! [[ $(git diff HEAD..master -- CHANGELOG.md) ]]
then
    echo "Error: CHANGELOG.md has not been updated"
    exit 1
fi


# Ensure __init__.py has changed
if ! [[ $(git diff HEAD..master -- merlin/__init__.py) ]]
then
    echo "Error: merlin/__init__.py has not been updated (version?)"
    exit 1
fi


exit 0
