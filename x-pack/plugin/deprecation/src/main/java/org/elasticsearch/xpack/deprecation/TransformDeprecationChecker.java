package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.List;

public class TransformDeprecationChecker implements DeprecationChecker {

    public static final String TRANSFORM_DEPRECATION_KEY = "transform_settings";

    @Override
    public boolean enabled(Settings settings) {
        // always enabled
        return true;
    }

    @Override
    public void check(Components components, ActionListener<CheckResult> deprecationIssueListener) {

        PageParams startPage = new PageParams(0, PageParams.DEFAULT_SIZE);
        List<DeprecationIssue> issues = new ArrayList<>();
        recursiveGetTransformsAndCollectDeprecations(
            components,
            issues,
            startPage,
            ActionListener.wrap(
                allIssues -> { deprecationIssueListener.onResponse(new CheckResult(getName(), allIssues)); },
                deprecationIssueListener::onFailure
            )
        );
    }

    @Override
    public String getName() {
        return TRANSFORM_DEPRECATION_KEY;
    }

    private void recursiveGetTransformsAndCollectDeprecations(
        Components components,
        List<DeprecationIssue> issues,
        PageParams page,
        ActionListener<List<DeprecationIssue>> listener
    ) {
        final GetTransformAction.Request request = new GetTransformAction.Request(Metadata.ALL);
        request.setPageParams(page);
        request.setAllowNoResources(true);

        components.client().execute(GetTransformAction.INSTANCE, request, ActionListener.wrap(getTransformResponse -> {
            for (TransformConfig config : getTransformResponse.getTransformConfigurations()) {
                issues.addAll(config.checkForDeprecations(components.xContentRegistry()));
            }
            if (getTransformResponse.getCount() >= (page.getFrom() + page.getSize())) {
                PageParams nextPage = new PageParams(page.getFrom() + page.getSize(), PageParams.DEFAULT_SIZE);
                recursiveGetTransformsAndCollectDeprecations(components, issues, nextPage, listener);
            } else {
                listener.onResponse(issues);
            }

        }, listener::onFailure));
    }
}
