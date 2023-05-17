
�Broot"_tf_keras_network*�B{"name": "model_17", "trainable": true, "expects_training_arg": true, "dtype": "float32", "batch_input_shape": null, "must_restore_from_config": false, "preserve_input_structure_in_config": false, "autocast": false, "class_name": "Functional", "config": {"name": "model_17", "layers": [{"class_name": "InputLayer", "config": {"batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 52]}, "dtype": "float32", "sparse": false, "ragged": false, "name": "csi_input"}, "name": "csi_input", "inbound_nodes": []}, {"class_name": "InputLayer", "config": {"batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 9]}, "dtype": "float32", "sparse": false, "ragged": false, "name": "light_input"}, "name": "light_input", "inbound_nodes": []}, {"class_name": "Concatenate", "config": {"name": "merged_input", "trainable": true, "dtype": "float32", "axis": 2}, "name": "merged_input", "inbound_nodes": [[["csi_input", 0, 0, {}], ["light_input", 0, 0, {}]]]}, {"class_name": "LSTM", "config": {"name": "lstm_17", "trainable": true, "batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 61]}, "dtype": "float32", "return_sequences": false, "return_state": false, "go_backwards": false, "stateful": false, "unroll": false, "time_major": false, "units": 48, "activation": "tanh", "recurrent_activation": "sigmoid", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 3}, "recurrent_initializer": {"class_name": "Orthogonal", "config": {"gain": 1.0, "seed": null}, "shared_object_id": 4}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 5}, "unit_forget_bias": true, "kernel_regularizer": null, "recurrent_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "recurrent_constraint": null, "bias_constraint": null, "dropout": 0.0, "recurrent_dropout": 0.0, "implementation": 2}, "name": "lstm_17", "inbound_nodes": [[["merged_input", 0, 0, {}]]]}, {"class_name": "Dropout", "config": {"name": "dropout_7", "trainable": true, "dtype": "float32", "rate": 0.1, "noise_shape": null, "seed": null}, "name": "dropout_7", "inbound_nodes": [[["lstm_17", 0, 0, {}]]]}, {"class_name": "Dense", "config": {"name": "dense_34", "trainable": true, "dtype": "float32", "units": 16, "activation": "relu", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}}, "bias_initializer": {"class_name": "Zeros", "config": {}}, "kernel_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "bias_constraint": null}, "name": "dense_34", "inbound_nodes": [[["dropout_7", 0, 0, {}]]]}, {"class_name": "Dense", "config": {"name": "dense_35", "trainable": true, "dtype": "float32", "units": 4, "activation": "softmax", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}}, "bias_initializer": {"class_name": "Zeros", "config": {}}, "kernel_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "bias_constraint": null}, "name": "dense_35", "inbound_nodes": [[["dense_34", 0, 0, {}]]]}], "input_layers": [["csi_input", 0, 0], ["light_input", 0, 0]], "output_layers": [["dense_35", 0, 0]]}, "shared_object_id": 15, "input_spec": [{"class_name": "InputSpec", "config": {"dtype": null, "shape": {"class_name": "__tuple__", "items": [null, 100, 52]}, "ndim": 3, "max_ndim": null, "min_ndim": null, "axes": {}}}, {"class_name": "InputSpec", "config": {"dtype": null, "shape": {"class_name": "__tuple__", "items": [null, 100, 9]}, "ndim": 3, "max_ndim": null, "min_ndim": null, "axes": {}}}], "build_input_shape": [{"class_name": "TensorShape", "items": [null, 100, 52]}, {"class_name": "TensorShape", "items": [null, 100, 9]}], "is_graph_network": true, "full_save_spec": {"class_name": "__tuple__", "items": [[[{"class_name": "TypeSpec", "type_spec": "tf.TensorSpec", "serialized": [{"class_name": "TensorShape", "items": [null, 100, 52]}, "float32", "csi_input"]}, {"class_name": "TypeSpec", "type_spec": "tf.TensorSpec", "serialized": [{"class_name": "TensorShape", "items": [null, 100, 9]}, "float32", "light_input"]}]], {}]}, "save_spec": [{"class_name": "TypeSpec", "type_spec": "tf.TensorSpec", "serialized": [{"class_name": "TensorShape", "items": [null, 100, 52]}, "float32", "csi_input"]}, {"class_name": "TypeSpec", "type_spec": "tf.TensorSpec", "serialized": [{"class_name": "TensorShape", "items": [null, 100, 9]}, "float32", "light_input"]}], "keras_version": "2.10.0", "backend": "tensorflow", "model_config": {"class_name": "Functional", "config": {"name": "model_17", "layers": [{"class_name": "InputLayer", "config": {"batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 52]}, "dtype": "float32", "sparse": false, "ragged": false, "name": "csi_input"}, "name": "csi_input", "inbound_nodes": [], "shared_object_id": 0}, {"class_name": "InputLayer", "config": {"batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 9]}, "dtype": "float32", "sparse": false, "ragged": false, "name": "light_input"}, "name": "light_input", "inbound_nodes": [], "shared_object_id": 1}, {"class_name": "Concatenate", "config": {"name": "merged_input", "trainable": true, "dtype": "float32", "axis": 2}, "name": "merged_input", "inbound_nodes": [[["csi_input", 0, 0, {}], ["light_input", 0, 0, {}]]], "shared_object_id": 2}, {"class_name": "LSTM", "config": {"name": "lstm_17", "trainable": true, "batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 61]}, "dtype": "float32", "return_sequences": false, "return_state": false, "go_backwards": false, "stateful": false, "unroll": false, "time_major": false, "units": 48, "activation": "tanh", "recurrent_activation": "sigmoid", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 3}, "recurrent_initializer": {"class_name": "Orthogonal", "config": {"gain": 1.0, "seed": null}, "shared_object_id": 4}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 5}, "unit_forget_bias": true, "kernel_regularizer": null, "recurrent_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "recurrent_constraint": null, "bias_constraint": null, "dropout": 0.0, "recurrent_dropout": 0.0, "implementation": 2}, "name": "lstm_17", "inbound_nodes": [[["merged_input", 0, 0, {}]]], "shared_object_id": 7}, {"class_name": "Dropout", "config": {"name": "dropout_7", "trainable": true, "dtype": "float32", "rate": 0.1, "noise_shape": null, "seed": null}, "name": "dropout_7", "inbound_nodes": [[["lstm_17", 0, 0, {}]]], "shared_object_id": 8}, {"class_name": "Dense", "config": {"name": "dense_34", "trainable": true, "dtype": "float32", "units": 16, "activation": "relu", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 9}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 10}, "kernel_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "bias_constraint": null}, "name": "dense_34", "inbound_nodes": [[["dropout_7", 0, 0, {}]]], "shared_object_id": 11}, {"class_name": "Dense", "config": {"name": "dense_35", "trainable": true, "dtype": "float32", "units": 4, "activation": "softmax", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 12}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 13}, "kernel_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "bias_constraint": null}, "name": "dense_35", "inbound_nodes": [[["dense_34", 0, 0, {}]]], "shared_object_id": 14}], "input_layers": [["csi_input", 0, 0], ["light_input", 0, 0]], "output_layers": [["dense_35", 0, 0]]}}, "training_config": {"loss": "categorical_crossentropy", "metrics": [[{"class_name": "MeanMetricWrapper", "config": {"name": "accuracy", "dtype": "float32", "fn": "categorical_accuracy"}, "shared_object_id": 18}]], "weighted_metrics": null, "loss_weights": null, "optimizer_config": {"class_name": "Adam", "config": {"name": "Adam", "learning_rate": 9.999999747378752e-05, "decay": 0.0, "beta_1": 0.8999999761581421, "beta_2": 0.9990000128746033, "epsilon": 1e-07, "amsgrad": false}}}}2
�root.layer-0"_tf_keras_input_layer*�{"class_name": "InputLayer", "name": "csi_input", "dtype": "float32", "sparse": false, "ragged": false, "batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 52]}, "config": {"batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 52]}, "dtype": "float32", "sparse": false, "ragged": false, "name": "csi_input"}}2
�root.layer-1"_tf_keras_input_layer*�{"class_name": "InputLayer", "name": "light_input", "dtype": "float32", "sparse": false, "ragged": false, "batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 9]}, "config": {"batch_input_shape": {"class_name": "__tuple__", "items": [null, 100, 9]}, "dtype": "float32", "sparse": false, "ragged": false, "name": "light_input"}}2
�root.layer-2"_tf_keras_layer*�{"name": "merged_input", "trainable": true, "expects_training_arg": false, "dtype": "float32", "batch_input_shape": null, "stateful": false, "must_restore_from_config": false, "preserve_input_structure_in_config": false, "autocast": true, "class_name": "Concatenate", "config": {"name": "merged_input", "trainable": true, "dtype": "float32", "axis": 2}, "inbound_nodes": [[["csi_input", 0, 0, {}], ["light_input", 0, 0, {}]]], "shared_object_id": 2, "build_input_shape": [{"class_name": "TensorShape", "items": [null, 100, 52]}, {"class_name": "TensorShape", "items": [null, 100, 9]}]}2
�
�root.layer-4"_tf_keras_layer*�{"name": "dropout_7", "trainable": true, "expects_training_arg": true, "dtype": "float32", "batch_input_shape": null, "stateful": false, "must_restore_from_config": false, "preserve_input_structure_in_config": false, "autocast": true, "class_name": "Dropout", "config": {"name": "dropout_7", "trainable": true, "dtype": "float32", "rate": 0.1, "noise_shape": null, "seed": null}, "inbound_nodes": [[["lstm_17", 0, 0, {}]]], "shared_object_id": 8, "build_input_shape": {"class_name": "TensorShape", "items": [null, 48]}}2
�root.layer_with_weights-1"_tf_keras_layer*�{"name": "dense_34", "trainable": true, "expects_training_arg": false, "dtype": "float32", "batch_input_shape": null, "stateful": false, "must_restore_from_config": false, "preserve_input_structure_in_config": false, "autocast": true, "class_name": "Dense", "config": {"name": "dense_34", "trainable": true, "dtype": "float32", "units": 16, "activation": "relu", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 9}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 10}, "kernel_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "bias_constraint": null}, "inbound_nodes": [[["dropout_7", 0, 0, {}]]], "shared_object_id": 11, "input_spec": {"class_name": "InputSpec", "config": {"dtype": null, "shape": null, "ndim": null, "max_ndim": null, "min_ndim": 2, "axes": {"-1": 48}}, "shared_object_id": 20}, "build_input_shape": {"class_name": "TensorShape", "items": [null, 48]}}2
�root.layer_with_weights-2"_tf_keras_layer*�{"name": "dense_35", "trainable": true, "expects_training_arg": false, "dtype": "float32", "batch_input_shape": null, "stateful": false, "must_restore_from_config": false, "preserve_input_structure_in_config": false, "autocast": true, "class_name": "Dense", "config": {"name": "dense_35", "trainable": true, "dtype": "float32", "units": 4, "activation": "softmax", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 12}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 13}, "kernel_regularizer": null, "bias_regularizer": null, "activity_regularizer": null, "kernel_constraint": null, "bias_constraint": null}, "inbound_nodes": [[["dense_34", 0, 0, {}]]], "shared_object_id": 14, "input_spec": {"class_name": "InputSpec", "config": {"dtype": null, "shape": null, "ndim": null, "max_ndim": null, "min_ndim": 2, "axes": {"-1": 16}}, "shared_object_id": 21}, "build_input_shape": {"class_name": "TensorShape", "items": [null, 16]}}2
�root.layer_with_weights-0.cell"_tf_keras_layer*�{"name": "lstm_cell_17", "trainable": true, "expects_training_arg": true, "dtype": "float32", "batch_input_shape": null, "stateful": false, "must_restore_from_config": false, "preserve_input_structure_in_config": false, "autocast": true, "class_name": "LSTMCell", "config": {"name": "lstm_cell_17", "trainable": true, "dtype": "float32", "units": 48, "activation": "tanh", "recurrent_activation": "sigmoid", "use_bias": true, "kernel_initializer": {"class_name": "GlorotUniform", "config": {"seed": null}, "shared_object_id": 3}, "recurrent_initializer": {"class_name": "Orthogonal", "config": {"gain": 1.0, "seed": null}, "shared_object_id": 4}, "bias_initializer": {"class_name": "Zeros", "config": {}, "shared_object_id": 5}, "unit_forget_bias": true, "kernel_regularizer": null, "recurrent_regularizer": null, "bias_regularizer": null, "kernel_constraint": null, "recurrent_constraint": null, "bias_constraint": null, "dropout": 0.0, "recurrent_dropout": 0.0, "implementation": 2}, "shared_object_id": 6, "build_input_shape": {"class_name": "__tuple__", "items": [null, 61]}}2
��root.keras_api.metrics.0"_tf_keras_metric*�{"class_name": "Mean", "name": "loss", "dtype": "float32", "config": {"name": "loss", "dtype": "float32"}, "shared_object_id": 22}2
��root.keras_api.metrics.1"_tf_keras_metric*�{"class_name": "MeanMetricWrapper", "name": "accuracy", "dtype": "float32", "config": {"name": "accuracy", "dtype": "float32", "fn": "categorical_accuracy"}, "shared_object_id": 18}2