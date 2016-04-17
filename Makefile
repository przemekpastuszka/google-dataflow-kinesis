prepare-local-maven:
	./tools/jar_installer.sh java-sdk-parent
	./tools/jar_installer.sh java-sdk-all
	./tools/jar_installer.sh sdks-parent
	./tools/jar_installer.sh parent
	./tools/jar_installer.sh runners-parent
	./tools/jar_installer.sh google-cloud-dataflow-java-runner

clean:
	./gradlew clean

build: prepare-local-maven
	./gradlew build

build-with-old-google:
	./gradlew build -PdataflowVersion=google