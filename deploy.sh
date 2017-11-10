set -eux
for project in `cat deploy.txt`; do
    cd $project
    while [[ 1 ]] ; do
        mvn clean deploy -DskipTests
        if [[ $? -eq 0 ]]; then
            break
        fi
    done
    cd -
done

