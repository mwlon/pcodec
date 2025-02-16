use jni::JNIEnv;
use jni::objects::JObject;
use pco::ChunkConfig;
use crate::mode::ModeSpecWrapper;

#[derive(Clone)]
pub struct ChunkConfigWrapper {
    compression_level: usize,
    mode_spec: ModeSpecWrapper,
}

impl ChunkConfigWrapper {
    pub fn from_jobject(env: &JNIEnv, obj: JObject) -> Self {
        let compression_level = env.get_field(obj, "compressionLevel", "I")
            .unwrap()
            .i()
            .unwrap() as usize;
        
        let mode_spec_obj = env.get_field(obj, "modeSpec", "Lcom/pco/Compressor$ModeSpec;")
            .unwrap()
            .l()
            .unwrap();
        let mode_spec = ModeSpecWrapper::from_jobject(env, mode_spec_obj);
        
        ChunkConfigWrapper {
            compression_level,
            mode_spec,
        }
    }
}

impl TryFrom<&ChunkConfigWrapper> for ChunkConfig {
    type Error = jni::errors::Error;

    fn try_from(config: &ChunkConfigWrapper) -> Result<Self, Self::Error> {
        let res = ChunkConfig::default()
            .with_compression_level(config.compression_level)
            .with_mode_spec(config.mode_spec.0.clone());
        Ok(res)
    }
} 