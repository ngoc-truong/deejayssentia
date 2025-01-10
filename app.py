from essentia.standard import MonoLoader, TensorflowPredictEffnetDiscogs, TensorflowPredict2D, RhythmExtractor2013
import numpy as np

audio = MonoLoader(filename="I'm Crazy 'Bout My Baby - Edit.mp3",
                   sampleRate=16000, resampleQuality=4)()


embedding_model = TensorflowPredictEffnetDiscogs(
    graphFilename="models/discogs-effnet-bs64-1.pb", output="PartitionedCall:1")
embeddings = embedding_model(audio)

model = TensorflowPredict2D(
    graphFilename="models/voice_instrumental-discogs-effnet-1.pb", output="model/Softmax", batchSize=-1)
predictions = model(embeddings)
print(predictions)

print(len(predictions))

singing_count = np.sum(predictions[:, 1] > 0.5)
print(f"Gesangigkeit: {singing_count / len(predictions)}")
