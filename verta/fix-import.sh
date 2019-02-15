for filename in verta/protos/modeldb/*_pb2*; do
    sed -i '' 's|from protos\.modeldb import|from . import|g' $filename
done
