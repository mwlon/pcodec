use numpy::PyArrayDyn;
use pco::standalone::{auto_compress, simple_decompress_into};
use pco::DEFAULT_COMPRESSION_LEVEL;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyObject, PyResult, Python};
use pyo3::types::PyBytes;

// The Numpy crate recommends using this type of enum to write functions that accept different Numpy dtypes
// https://github.com/PyO3/rust-numpy/blob/32740b33ec55ef0b7ebec726288665837722841d/examples/simple/src/lib.rs#L113
// Subsequently, we have to do a lot of repetitive `match` statements to handle the different dtypes.
// There has to be a better way the involves less repetition, but I don't know enough Rust to figure it out.
#[derive(FromPyObject)]
enum ArrayDynFloat<'py> {
    F32(&'py PyArrayDyn<f32>),
    F64(&'py PyArrayDyn<f64>),
}

#[pymodule]
fn pcodec(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    
    #[pyfn(m)]
    fn compress<'py>(py: Python<'py>, x: ArrayDynFloat<'py>) -> PyObject {
        match x {
            ArrayDynFloat::F32(x) => {
                let ro = x.readonly();
                let array = ro.as_array();
                let slice = array.as_slice().unwrap();
                let compressed: Vec<u8> = auto_compress(&slice, DEFAULT_COMPRESSION_LEVEL);
                PyBytes::new(py, &compressed).into()
            }
            ArrayDynFloat::F64(x) => {
                let ro = x.readonly();
                let array = ro.as_array();
                let slice = array.as_slice().unwrap();
                let compressed: Vec<u8> = auto_compress(&slice, DEFAULT_COMPRESSION_LEVEL);
                PyBytes::new(py, &compressed).into()
            }
        }
    }

    #[pyfn(m)]
    fn decompress<'py>(compressed: &PyBytes, out: ArrayDynFloat<'py>) -> PyResult<()> {
        match out {
            ArrayDynFloat::F32(out) => {
                let mut out_rw = out.readwrite();
                let dst = out_rw.as_slice_mut().expect("failed to get mutable slice");
                let src: &[u8] = compressed.extract().unwrap();
                let progress =
                    simple_decompress_into::<f32>(src, dst).expect("failed to decompress");
                if progress.finished != true {
                    Err(PyRuntimeError::new_err(
                        "decompression didn't finish. Buffer too small?",
                    ))
                } else {
                    Ok(())
                }
            }
            ArrayDynFloat::F64(out) => {
                let mut out_rw = out.readwrite();
                let dst = out_rw.as_slice_mut().expect("failed to get mutable slice");
                let src: &[u8] = compressed.extract().unwrap();
                let progress =
                    simple_decompress_into::<f64>(src, dst).expect("failed to decompress");
                if progress.finished != true {
                    Err(PyRuntimeError::new_err(
                        "decompression didn't finish. Buffer too small?",
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    Ok(())
}
