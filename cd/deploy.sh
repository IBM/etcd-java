#!/usr/bin/env bash

echo TRAVIS_JDK_VERSION="$TRAVIS_JDK_VERSION"
if [ "$TRAVIS_BRANCH" = 'main' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ] && [ "$TRAVIS_JDK_VERSION" == 'openjdk17' ] && [ "$TARGET_ETCD_VERSION" == 'v3.5.10' ]; then
    echo "deploying release to central repository"

    # prepare key for signing
    openssl aes-256-cbc -K $encrypted_80546c16ab97_key -iv $encrypted_80546c16ab97_iv -in cd/signingkey.asc.enc -out cd/signingkey.asc -d
    gpg --fast-import --batch cd/signingkey.asc
    shred --remove cd/signingkey.asc
    
    # sign and deploy to central repository
    mvn deploy -P release-artifacts -DskipTests=true --settings cd/mvnsettings.xml
fi
