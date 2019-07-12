"""
The following script writes to the output files all variables that are of type NC_DOUBLE and that have at least two dimensions. It then changes their _FillValue to 1.0E-9. The function get_vars_in() creates an NC_STRING attribute that contains all of the variable names in the input file. Note that a vpointer must be a plain attribute, NOT an a attribute expression. Thus in the below script using *all(idx) would be a fundamental mistake. In the below example the vpointer var_nm is of type NC_STRING.

    @all=get_vars_in();

    *sz=@all.size();
    *idx=0;

    for(idx=0;idx<sz;idx++){
      // @var_nm is of type NC_STRING
      @var_nm=@all(idx);

      if(*@var_nm.ndims() >= 3){
         *@var_nm=*@var_nm;
         *@var_nm=idx;
    //     *@var_nm.change_miss(1e-9d);
      }
    }

The following commands were used to shrink NetCDF files by replacing their
data variables with a constant and compression floating point variables.

    ncks -4 -L 5 shrunken_fc.nc new.nc
    ncks -4 --deflate 5 --cnk_dmn=x,1000 --cnk_dmn=y,1000 in.nc out.nc
    ncks -4 --ppc default=2

    ncap2 -4 -L 5 --script-file
"""
from click.testing import CliRunner

from dea_cog_converter.cli import cli


def test_cli_can_perform_a_simple_conversion():
    pass


def test_cli_can_save_an_s3_inventory():
    pass


def test_cli_can_generate_a_work_list():
    runner = CliRunner()
    result = runner.invoke(cli, ['generate-work-list', '--help'])

    print(result.output)
    assert result.exit_code == 0


def test_cli_can_run_a_parallel_conversion_using_mpi():
    pass


def test_cli_can_verify_generated_cogs():
    pass


def test_cli_can_self_submit_using_pbs_qsub():
    pass


def test_get_param_names():
    pass


def test_check_prefix_from_query_result():
    pass


def test_netcdfcogconverter():
    pass

