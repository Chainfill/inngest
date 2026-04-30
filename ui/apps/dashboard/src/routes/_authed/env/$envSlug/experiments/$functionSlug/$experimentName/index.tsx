import { createFileRoute } from '@tanstack/react-router';
import { useCallback, useMemo } from 'react';
import type { RangeChangeProps } from '@inngest/components/DatePicker/RangePicker';

import NotFound from '@/components/Error/NotFound';
import { ExperimentDetailPage } from '@/components/Experiments/ExperimentDetailPage';
import { useBooleanFlag } from '@/components/FeatureFlags/hooks';
import {
  getExperimentUrlState,
  hasExperimentTimeRangeSearch,
  setExperimentPanelSearch,
  setExperimentScoringFormulaSearch,
  setExperimentShowInactiveSearch,
  setExperimentTimeRangeSearch,
  setExperimentVariantsSearch,
  validateExperimentDetailSearch,
  type ExperimentDetailPanel,
} from '@/lib/experiments/urlState';
import { useFunction } from '@/queries/functions';

export const Route = createFileRoute(
  '/_authed/env/$envSlug/experiments/$functionSlug/$experimentName/',
)({
  component: ExperimentDetailRoute,
  validateSearch: validateExperimentDetailSearch,
});

function ExperimentDetailRoute() {
  const { functionSlug, experimentName } = Route.useParams();
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  const experimentsEnabled = useBooleanFlag('experimentation-steps');
  const urlState = useMemo(() => getExperimentUrlState(search), [search]);
  const hasTimeRangeSearch = hasExperimentTimeRangeSearch(search);

  const updateTimeRange = useCallback(
    (range: RangeChangeProps) => {
      navigate({
        search: (prev) => setExperimentTimeRangeSearch(prev, range),
        replace: true,
      });
    },
    [navigate],
  );

  const updateSelectedVariants = useCallback(
    (variants: string[]) => {
      navigate({
        search: (prev) => setExperimentVariantsSearch(prev, variants),
        replace: true,
      });
    },
    [navigate],
  );

  const updateShowInactive = useCallback(
    (showInactive: boolean) => {
      navigate({
        search: (prev) => setExperimentShowInactiveSearch(prev, showInactive),
        replace: true,
      });
    },
    [navigate],
  );

  const updateActivePanel = useCallback(
    (panel: ExperimentDetailPanel) => {
      navigate({
        search: (prev) => setExperimentPanelSearch(prev, panel),
        replace: true,
      });
    },
    [navigate],
  );

  const updateScoringFormula = useCallback(
    (formulaParam: string | undefined) => {
      navigate({
        search: (prev) => setExperimentScoringFormulaSearch(prev, formulaParam),
        replace: true,
      });
    },
    [navigate],
  );

  const [{ data, fetching }] = useFunction({
    functionSlug: decodeURIComponent(functionSlug),
  });
  const functionID = data?.workspace.workflow?.id ?? null;

  if (experimentsEnabled.isReady && !experimentsEnabled.value) {
    return <NotFound />;
  }

  if (!fetching && functionID === null) {
    return <NotFound />;
  }

  if (functionID === null) {
    return null;
  }

  return (
    <ExperimentDetailPage
      experimentName={decodeURIComponent(experimentName)}
      functionID={functionID}
      timeRange={urlState.timeRange}
      hasTimeRangeSearch={hasTimeRangeSearch}
      onTimeRangeChange={updateTimeRange}
      selectedVariants={urlState.selectedVariants}
      onSelectedVariantsChange={updateSelectedVariants}
      showInactive={urlState.showInactive}
      onShowInactiveChange={updateShowInactive}
      activePanel={urlState.panel}
      onActivePanelChange={updateActivePanel}
      scoreFormula={urlState.scoreFormula}
      scoreFormulaParam={urlState.scoreFormulaParam}
      onScoringFormulaChange={updateScoringFormula}
    />
  );
}
