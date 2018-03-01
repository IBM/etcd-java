#!/usr/bin/env bash
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
	
	# prepare key for signing
    openssl aes-256-cbc -K $encrypted_d363c995e9f6_key -iv $encrypted_d363c995e9f6_iv -in cd/signingkey.asc.enc -out cd/signingkey.asc -d
    gpg --fast-import cd/signingkey.asc
    
    # deploy to central repository
    mvn deploy -P release-artifacts --settings cd/mvnsettings.xml
fi
