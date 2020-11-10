CREATE TABLE `tbl_user_pin` (
  `id` int(11) NOT NULL,
  `nama` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

CREATE TABLE `tbl_activation` (
  `id` int(11) NOT NULL,
  `lokasi` varchar(255) NOT NULL,
  `alasan` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

CREATE TABLE `tbl_activation_mnt_log` (
  `id` int(11) NOT NULL,
  `lokasi_log` varchar(255) NOT NULL,
  `alasan_penghapusan` varchar(255) NOT NULL,
  `date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1