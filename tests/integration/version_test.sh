#!/bin/bash

# Ensure CHANGELOG version and merlin/__init__.py version are equal
grep CHANGELOG.md -m 1 -e "## \[[0-9]\+\.[0-9]\+\.[0-9]\+\]" > GREP1
grep merlin/__init__.py -m 1 -e "\"[0-9]\+\.[0-9]\+\.[0-9]\+\"" > GREP2
grep -o GREP1 -e "[0-9]\+\.[0-9]\+\.[0-9]\+" > GREP3
grep -o GREP2 -e "[0-9]\+\.[0-9]\+\.[0-9]\+" > GREP4
if ! cmp GREP3 GREP4
then 
    echo "Error: merlin/__init__.py version different from CHANGELOG.md verison" 
    exit 1
fi


# Ensure CHANGELOG version and git tag version are equal
git tag > TAGS
grep "." TAGS | tail -1 > TAG
grep CHANGELOG.md -m 1 -e "## \[[0-9]\+\.[0-9]\+\.[0-9]\+\]" > GREP1
grep -o GREP1 -e "[0-9]\+\.[0-9]\+\.[0-9]\+" > GREP2
if ! cmp TAG GREP2
then 
    echo "Error: CHANGELOG.md verison different from most recent git tag" 
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
