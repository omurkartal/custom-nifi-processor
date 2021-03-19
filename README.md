# Custom Nifi Processor

How will you create this kind of project?

>mvn archetype:generate -DarchetypeGroupId=org.apache.nifi -DarchetypeArtifactId=nifi-processor-bundle-archetype -DarchetypeVersion=1.9.0 -DnifiVersion=1.9.0

- Define value for property 'groupId': edu.omur.nifi
- Define value for property 'artifactId': custom-nifi-processor
- Define value for property 'version' 1.0-SNAPSHOT: :
- Define value for property 'artifactBaseName': sample
- Define value for property 'package' edu.omur.nifi.processors.sample: : edu.omur.nifi.processors

## Note-1
Class name of the processor must be stated in `org.apache.nifi.processor.Processor` file which is under ```resources/META-INF.services``` folder.

## Deployment-Note
Build the project.<br/>
Copy the output <b>.nar</b> file to the `lib` folder of nifi installation. (.nar file is in target folder of nifi-sample-nar module)<br>
After nifi service is restarted, our custom processor will be shown in nifi.
