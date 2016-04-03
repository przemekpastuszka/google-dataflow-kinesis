prepare-local-maven:
	./tools/jar_installer.sh java-sdk-parent
	./tools/jar_installer.sh java-sdk-all
	./tools/jar_installer.sh sdks-parent
	./tools/jar_installer.sh parent

clean:
	./gradlew clean

build: prepare-local-maven
	./gradlew build

build-with-old-google:
	./gradlew build -PdataflowVersion=google