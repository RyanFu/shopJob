call mvn clean install -Dtest -DfailIfNoTests=false
cd target
rename dueam-jobs.jar dueam-jobs.jar
@pause