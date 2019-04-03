defmodule Indexer.Supervisor do
  @moduledoc """
  Supervisor of all indexer worker supervision trees
  """

  use Supervisor

  alias Indexer.{
    Block,
    CoinBalance,
    Token
  }

  alias Indexer.Fetcher.{
    BlockReward,
    ContractCode,
    InternalTransaction,
    PendingTransaction,
    ReplacedTransaction,
    TokenBalance
  }

  alias Indexer.Temporary.UncatalogedTokenTransfers

  def child_spec([]) do
    child_spec([[]])
  end

  def child_spec([init_arguments]) do
    child_spec([init_arguments, []])
  end

  def child_spec([_init_arguments, _gen_server_options] = start_link_arguments) do
    default = %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, start_link_arguments},
      type: :supervisor
    }

    Supervisor.child_spec(default, [])
  end

  def start_link(arguments, gen_server_options \\ []) do
    Supervisor.start_link(__MODULE__, arguments, Keyword.put_new(gen_server_options, :name, __MODULE__))
  end

  @impl Supervisor
  def init(%{memory_monitor: memory_monitor}) do
    json_rpc_named_arguments = Application.fetch_env!(:indexer, :json_rpc_named_arguments)

    block_fetcher_supervisor_named_arguments =
      :indexer
      |> Application.get_all_env()
      |> Keyword.take(
        ~w(blocks_batch_size blocks_concurrency block_interval json_rpc_named_arguments receipts_batch_size
           receipts_concurrency subscribe_named_arguments realtime_overrides)a
      )
      |> Enum.into(%{})
      |> Map.put(:memory_monitor, memory_monitor)
      |> Map.put_new(:realtime_overrides, %{})

    Supervisor.init(
      [
        {CoinBalance.Supervisor,
         [
           [json_rpc_named_arguments: json_rpc_named_arguments, memory_monitor: memory_monitor],
           [name: CoinBalance.Supervisor]
         ]},
        {PendingTransaction.Supervisor, [[json_rpc_named_arguments: json_rpc_named_arguments]]},
        {ContractCode.Supervisor,
         [[json_rpc_named_arguments: json_rpc_named_arguments, memory_monitor: memory_monitor]]},
        {ReplacedTransaction.Supervisor, [[memory_monitor: memory_monitor]]},
        {InternalTransaction.Supervisor,
         [[json_rpc_named_arguments: json_rpc_named_arguments, memory_monitor: memory_monitor]]},
        {Token.Supervisor,
         [
           [json_rpc_named_arguments: json_rpc_named_arguments, memory_monitor: memory_monitor],
           [name: Token.Supervisor]
         ]},
        {TokenBalance.Supervisor,
         [[json_rpc_named_arguments: json_rpc_named_arguments, memory_monitor: memory_monitor]]},
        {Block.Supervisor, [block_fetcher_supervisor_named_arguments, [name: Block.Supervisor]]},
        {BlockReward.Supervisor,
         [[json_rpc_named_arguments: json_rpc_named_arguments, memory_monitor: memory_monitor]]},
        {UncatalogedTokenTransfers, [[]]}
      ],
      strategy: :one_for_one
    )
  end
end
