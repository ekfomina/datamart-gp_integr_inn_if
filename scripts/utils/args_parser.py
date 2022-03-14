def parse_arguments(parser):
    parser.add_argument("--loading_id",
                        dest="loading_id",
                        type=int,
                        help="loading_id from CTL",
                        required=True)
    parser.add_argument("--etl_force_load",
                        dest="etl_force_load",
                        type=int,
                        help="flag of archive loading",
                        required=True)
    parser.add_argument("--etl_src_table_1",
                        dest="etl_src_table_1",
                        type=str,
                        help="path to table",
                        required=True)
    parser.add_argument("--etl_src_dir_1",
                        dest="etl_src_dir_1",
                        type=str,
                        help="path to partition's directory",
                        required=True)
    parser.add_argument("--etl_pa_table_1",
                        dest="etl_pa_table_1",
                        type=str,
                        help="path to table",
                        required=True)
    parser.add_argument("--etl_pa_dir_1",
                        dest="etl_pa_dir_1",
                        type=str,
                        help="path to partition's directory",
                        required=True)
    parser.add_argument("--etl_src_table_2",
                        dest="etl_src_table_2",
                        type=str,
                        help="path to table",
                        required=True)
    parser.add_argument("--etl_src_dir_2",
                        dest="etl_src_dir_2",
                        type=str,
                        help="path to partition's directory",
                        required=True)
    parser.add_argument("--etl_pa_table_2",
                        dest="etl_pa_table_2",
                        type=str,
                        help="path to table",
                        required=True)
    parser.add_argument("--etl_pa_dir_2",
                        dest="etl_pa_dir_2",
                        type=str,
                        help="path to partition's directory",
                        required=True)
    parser.add_argument("--name_node",
                        dest="name_node",
                        type=str,
                        help="path to name node",
                        required=True)
    parser.add_argument("--ctl_url",
                        dest="ctl_url",
                        type=str,
                        help="ctl url for use rest api",
                        required=True)
    parser.add_argument("--ctl_entity_id",
                        dest="ctl_entity_id",
                        type=int,
                        help="id ctl workflow",
                        required=True)
    parser.add_argument("--fake_loading",
                        dest="fake_loading",
                        type=int,
                        help="fake_loading",
                        required=True)

    known_args, unknown_args = parser.parse_known_args()

    return known_args
