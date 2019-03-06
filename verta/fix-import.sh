for filename in 'verta/_protos/public/modeldb/*_pb2*'; do
    sed -i '' 's|from protos\.public\.modeldb import|from . import|g' $filename
done
