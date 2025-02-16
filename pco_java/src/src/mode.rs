use jni::JNIEnv;
use jni::objects::JObject;
use pco::ModeSpec;

#[derive(Clone, Default)]
pub struct ModeSpecWrapper(ModeSpec);

impl ModeSpecWrapper {
    pub fn from_jobject(env: &JNIEnv, obj: JObject) -> Self {
        let variant = env.get_field(obj, "variant", "I")
            .unwrap()
            .i()
            .unwrap();
        
        Self(match variant {
            0 => ModeSpec::Auto,
            1 => ModeSpec::Classic,
            2 => {
                let base = env.get_field(obj, "floatBase", "D")
                    .unwrap()
                    .d()
                    .unwrap();
                ModeSpec::TryFloatMult(base)
            },
            3 => {
                let k = env.get_field(obj, "k", "I")
                    .unwrap()
                    .i()
                    .unwrap() as u32;
                ModeSpec::TryFloatQuant(k)
            },
            4 => {
                let base = env.get_field(obj, "intBase", "J")
                    .unwrap()
                    .j()
                    .unwrap() as u64;
                ModeSpec::TryIntMult(base)
            },
            _ => unreachable!("Invalid ModeSpec variant"),
        })
    }
} 